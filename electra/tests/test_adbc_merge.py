import argparse
import concurrent.futures
import csv
import ctypes
import json
import platform
import tempfile
import threading
import uuid
from pathlib import Path

import psycopg
from adbc_driver_manager import dbapi
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def execute(connection, statement, parameters=None):
    cursor = connection.cursor()
    try:
        cursor.execute(statement, parameters)
    finally:
        cursor.close()


def fetchone(connection, statement, parameters=None):
    cursor = connection.cursor()
    try:
        cursor.execute(statement, parameters)
        return cursor.fetchone()
    finally:
        cursor.close()


def fetchall(connection, statement, parameters=None):
    cursor = connection.cursor()
    try:
        cursor.execute(statement, parameters)
        return cursor.fetchall()
    finally:
        cursor.close()


def validate_dsn(parser, dsn):
    options = conninfo_to_dict(dsn)
    allowed = {"host", "port", "dbname", "user", "password"}
    if options.keys() - allowed:
        parser.error("Only host, port, dbname, user, and password are accepted")
    if (
        options.get("host") != "127.0.0.1"
        or options.get("port") != "15545"
        or options.get("dbname") != "lake_writer_repro"
    ):
        parser.error("Use the disposable database lake_writer_repro at 127.0.0.1:15545")
    return make_conninfo(**options, hostaddr="127.0.0.1")


def open_connection(driver, extension, extension_cache):
    ctypes.CDLL(str(driver), mode=ctypes.RTLD_GLOBAL)
    connection = dbapi.connect(
        driver=str(driver),
        entrypoint="duckdb_adbc_init",
        db_kwargs={"allow_unsigned_extensions": "true"},
        autocommit=True,
    )
    try:
        execute(connection, "SET threads = 2")
        execute(connection, f"SET extension_directory = {literal(extension_cache)}")
        execute(connection, "LOAD httpfs")
        execute(connection, "LOAD postgres")
        execute(connection, f"LOAD {literal(extension)}")
        connection.adbc_connection.set_autocommit(True)
    except Exception:
        connection.close()
        raise
    return connection


def attach_lake(connection, dsn, schema, data_directory):
    attach = (
        f"ATTACH {literal('ducklake:postgres:' + dsn)} AS lake "
        f"(METADATA_SCHEMA {literal(schema)}, DATA_PATH {literal(data_directory)}, "
        "DATA_INLINING_ROW_LIMIT 0)"
    )
    execute(connection, attach)


def source_query(rows):
    values = ", ".join(
        f"({row_id}, {literal(value)}, {str(deleted).upper()})"
        for row_id, value, deleted in rows
    )
    return (
        "CREATE TEMP TABLE merge_source AS "
        f"SELECT * FROM (VALUES {values}) AS source(id, value, deleted)"
    )


def merge_rows(connection, rows):
    execute(connection, "DROP TABLE IF EXISTS merge_source")
    execute(connection, source_query(rows))
    if any(deleted for _, _, deleted in rows):
        if not all(deleted for _, _, deleted in rows):
            raise ValueError("Use separate MERGE statements for updates and deletes")
        action = "WHEN MATCHED THEN DELETE"
    else:
        action = (
            "WHEN MATCHED THEN UPDATE SET value = source.value, deleted = source.deleted "
            "WHEN NOT MATCHED THEN INSERT (id, value, deleted) "
            "VALUES (source.id, source.value, source.deleted)"
        )
    execute(
        connection,
        "MERGE INTO lake.events AS target USING merge_source AS source "
        "ON target.id = source.id " + action,
    )


def rollback(connection):
    try:
        execute(connection, "ROLLBACK")
    except dbapi.Error as error:
        if "no transaction is active" not in str(error).lower():
            raise


def is_conflict(error):
    return isinstance(error, dbapi.Error) and "transaction conflict - attempting to" in str(error).lower()


def disjoint_round(connection, identity, round_number, barrier):
    row_id = round_number * 100 + 50 + identity
    execute(connection, "SET ducklake_max_retry_count = 100")
    execute(connection, "BEGIN")
    merge_rows(connection, [(row_id, f"disjoint-{round_number}-{identity}", False)])
    barrier.wait(timeout=60)
    execute(connection, "COMMIT")
    if fetchone(connection, "SELECT 1") != (1,):
        raise AssertionError("ADBC connection failed after a disjoint MERGE")
    return row_id


def conflict_probe(connections, observer):
    barrier = threading.Barrier(2)

    def run(identity, connection):
        row_id = -10_000 - identity
        execute(connection, "SET ducklake_max_retry_count = 100")
        execute(connection, "BEGIN")
        merge_rows(
            connection,
            [
                (row_id, f"failed-{identity}", False),
                (0, f"probe-{identity}", False),
            ],
        )
        barrier.wait(timeout=60)
        try:
            execute(connection, "COMMIT")
        except Exception as error:
            if not is_conflict(error):
                rollback(connection)
                raise
            rollback(connection)
            if fetchone(connection, "SELECT count(*) FROM lake.events WHERE id = ?", [row_id]) != (0,):
                raise AssertionError("Rows from the failed MERGE remained visible")
            merge_rows_in_transaction(connection, [(-20_000 - identity, f"recovery-{identity}", False)])
            if fetchone(connection, "SELECT count(*) FROM lake.events WHERE id = ?", [-20_000 - identity]) != (1,):
                raise AssertionError("The failed MERGE connection could not recover")
            return {"conflict": True, "failed_row": row_id, "recovery_row": -20_000 - identity}
        return {"conflict": False, "failed_row": row_id, "recovery_row": None}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(run, range(2), connections[:2]))
    conflicts = sum(outcome["conflict"] for outcome in outcomes)
    if conflicts != 1:
        raise AssertionError({"reason": "The conflict probe did not produce one winner and one loser", "outcomes": outcomes})
    observed_winner = fetchall(
        observer,
        "SELECT id, value FROM lake.events WHERE id IN (-10000, -10001) ORDER BY id",
    )
    winner = next(identity for identity, outcome in enumerate(outcomes) if not outcome["conflict"])
    if observed_winner != [(-10_000 - winner, f"failed-{winner}")]:
        raise AssertionError({"reason": "Conflict winner row set is incorrect", "rows": observed_winner})
    if fetchone(observer, "SELECT value FROM lake.events WHERE id = 0") != (f"probe-{winner}",):
        raise AssertionError("The failed MERGE changed the winning update")
    recovery_ids = [outcome["recovery_row"] for outcome in outcomes if outcome["recovery_row"]]
    recovery_rows = fetchall(
        observer,
        "SELECT id, value FROM lake.events WHERE id IN (-20000, -20001) ORDER BY id",
    )
    if sorted(recovery_rows) != sorted([(recovery_ids[0], f"recovery-{-recovery_ids[0] - 20000}")]):
        raise AssertionError({"reason": "Conflict recovery row set is incorrect", "rows": recovery_rows})
    return {
        "conflicts": conflicts,
        "outcomes": outcomes,
        "winner_rows": observed_winner,
        "recovery_rows": recovery_rows,
    }


def merge_rows_in_transaction(connection, rows):
    execute(connection, "BEGIN")
    merge_rows(connection, rows)
    execute(connection, "COMMIT")


def overlapping_round(connections, round_number):
    barrier = threading.Barrier(len(connections))
    max_attempts = 8

    def run(identity, connection):
        conflicts = 0
        for attempt in range(max_attempts):
            execute(connection, "SET ducklake_max_retry_count = 100")
            execute(connection, "BEGIN")
            merge_rows(
                connection,
                [
                    (round_number * 100 + key, f"overlap-{round_number}-{identity}", False)
                    for key in range(8)
                ],
            )
            if attempt == 0:
                barrier.wait(timeout=60)
            try:
                execute(connection, "COMMIT")
            except Exception as error:
                rollback(connection)
                if not is_conflict(error):
                    raise
                conflicts += 1
                continue
            if fetchone(connection, "SELECT 1") != (1,):
                raise AssertionError("ADBC connection failed after an overlapping MERGE")
            return {"attempts": attempt + 1, "conflicts": conflicts}
        raise AssertionError({"reason": "Bounded MERGE retry limit exceeded", "identity": identity})

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(connections)) as executor:
        outcomes = list(executor.map(run, range(len(connections)), connections))
    rows = fetchall(
        connections[0],
        "SELECT id, value, count(*) FROM lake.events WHERE id >= ? AND id < ? GROUP BY id, value ORDER BY id",
        [round_number * 100, round_number * 100 + 8],
    )
    if len(rows) != 8 or any(count != 1 for _, _, count in rows):
        raise AssertionError({"reason": "Overlapping MERGE produced duplicate or missing rows", "rows": rows})
    values = {value for _, value, _ in rows}
    allowed_values = {f"overlap-{round_number}-{identity}" for identity in range(len(connections))}
    if len(values) != 1 or not values <= allowed_values:
        raise AssertionError({"reason": "Overlapping MERGE left partial updates", "rows": rows})
    conflicts = sum(outcome["conflicts"] for outcome in outcomes)
    if conflicts < len(connections) - 1:
        raise AssertionError("The synchronized overlapping MERGE transactions did not exercise conflicts")
    return {"outcomes": outcomes, "rows": len(rows), "conflicts": conflicts, "value": next(iter(values))}


def delete_and_recover(connection):
    execute(connection, "SET ducklake_max_retry_count = 100")
    execute(connection, "BEGIN")
    merge_rows(connection, [(0, "deleted", True), (1, "deleted", True)])
    execute(connection, "COMMIT")
    if fetchall(connection, "SELECT id FROM lake.events WHERE id IN (0, 1)"):
        raise AssertionError("MERGE DELETE left rows in the lake")
    merge_rows_in_transaction(
        connection,
        [(0, "recovered-0", False), (1, "recovered-1", False), (-30_000, "recovered-new", False)],
    )
    recovered = fetchall(
        connection,
        "SELECT id, value FROM lake.events WHERE id IN (0, 1, -30000) ORDER BY id",
    )
    if recovered != [(-30_000, "recovered-new"), (0, "recovered-0"), (1, "recovered-1")]:
        raise AssertionError({"reason": "Recovery MERGE did not restore exact rows", "rows": recovered})
    return {"deleted": 2, "recovered": 3}


def seed_overlap_rows(connection, round_number):
    merge_rows_in_transaction(
        connection,
        [
            (round_number * 100 + key, f"seed-{round_number}-{key}", False)
            for key in range(8)
        ],
    )


def verify_final_rows(observer, expected_ids, overlap_ids, fixed_values):
    duplicates = fetchall(
        observer,
        "SELECT id, count(*) FROM lake.events GROUP BY id HAVING count(*) <> 1 ORDER BY id",
    )
    if duplicates:
        raise AssertionError({"reason": "Final lake rows contain duplicate keys", "rows": duplicates})
    rows = fetchall(
        observer,
        "SELECT id, value, deleted, count(*) FROM lake.events GROUP BY id, value, deleted ORDER BY id",
    )
    if any(count != 1 or deleted for _, _, deleted, count in rows):
        raise AssertionError({"reason": "Final lake rows contain duplicates or deleted rows", "rows": rows})
    observed_ids = {row_id for row_id, _, _, _ in rows}
    if observed_ids != expected_ids:
        raise AssertionError(
            {
                "reason": "Final lake row set is incorrect",
                "missing": sorted(expected_ids - observed_ids),
                "unexpected": sorted(observed_ids - expected_ids),
            }
        )
    for row_id, value, _, _ in rows:
        if row_id in fixed_values and value != fixed_values[row_id]:
            raise AssertionError({"reason": "Final lake row has the wrong value", "id": row_id, "value": value})
        if row_id in overlap_ids and not value.startswith("overlap-"):
            raise AssertionError({"reason": "Overlapping MERGE value is invalid", "id": row_id, "value": value})
    return {"rows": len(rows), "overlap_keys": len(overlap_ids)}


def verify_postgres_log(log_path, schema):
    if not log_path.exists():
        raise AssertionError(f"PostgreSQL log does not exist: {log_path}")
    matches = 0
    with log_path.open(newline="") as log_file:
        for row in csv.reader(log_file):
            terms = (
                schema.lower(),
                "union all",
                "ducklake_snapshot_changes",
                "ducklake_table_stats",
                "max(snapshot_id)",
            )
            if len(row) > 13 and all(term in row[13].lower() for term in terms):
                matches += 1
    if matches == 0:
        raise AssertionError("PostgreSQL logs contain no combined DuckLake metadata query")
    return matches


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--extension", type=Path, required=True)
    parser.add_argument("--extension-cache", type=Path, required=True)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--data-directory", type=Path)
    parser.add_argument("--rounds", type=int, default=50)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--external-writer", action="store_true")
    parser.add_argument("--postgres-log", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine().lower() not in {"x86_64", "amd64"}:
        parser.error("Run this integration test on Linux AMD64")
    if args.rounds < 1 or not 2 <= args.workers <= 49:
        parser.error("Rounds must be positive and workers must be between two and 49")
    for path, label in ((args.driver, "driver"), (args.extension, "extension"), (args.extension_cache, "extension cache")):
        if not path.exists():
            parser.error(f"{label} does not exist: {path}")
    dsn = validate_dsn(parser, args.dsn)
    data_directory = args.data_directory
    temporary_data = None
    if data_directory is None:
        temporary_data = tempfile.TemporaryDirectory(prefix="ducklake-adbc-merge-")
        data_directory = Path(temporary_data.name)
    data_directory.mkdir(parents=True, exist_ok=True)
    metadata_schema = "adbc_merge_" + uuid.uuid4().hex
    summary = {
        "driver": str(args.driver),
        "extension": str(args.extension),
        "rounds": args.rounds,
        "workers": args.workers,
        "external_writer": args.external_writer,
    }
    connections = []
    try:
        with psycopg.connect(dsn, autocommit=True) as pg:
            if pg.execute("SELECT current_database()").fetchone()[0] != "lake_writer_repro":
                raise RuntimeError("Unexpected PostgreSQL database")
        root = open_connection(args.driver, args.extension, args.extension_cache)
        connections.append(root)
        connections.extend(root.adbc_clone() for _ in range(args.workers - 1))
        for connection in connections:
            connection.adbc_connection.set_autocommit(True)
        summary["duckdb_version"] = fetchone(root, "SELECT version()")[0]
        if summary["duckdb_version"] != "v1.5.5":
            raise AssertionError("Use the pinned DuckDB 1.5.5 ADBC driver")
        summary["loaded_extensions"] = fetchall(
            root, "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded ORDER BY extension_name"
        )
        attach_lake(root, dsn, metadata_schema, data_directory)
        execute(root, "CREATE TABLE lake.events (id INTEGER, value VARCHAR, deleted BOOLEAN)")
        for connection in connections[1:]:
            if fetchone(connection, "SELECT count(*) FROM lake.events") != (0,):
                raise AssertionError("Shared ADBC clone did not see the root DuckLake catalog")
        if args.external_writer:
            external = open_connection(args.driver, args.extension, args.extension_cache)
            attach_lake(external, dsn, metadata_schema, data_directory)
            connections.append(external)
        seed_overlap_rows(root, 0)
        summary["conflict_probe"] = conflict_probe(connections, connections[0])
        expected_ids = set(range(8))
        expected_ids.update(row_id for row_id, _ in summary["conflict_probe"]["winner_rows"])
        expected_ids.update(row_id for row_id, _ in summary["conflict_probe"]["recovery_rows"])
        fixed_values = {
            row_id: value
            for row_id, value in summary["conflict_probe"]["winner_rows"]
            + summary["conflict_probe"]["recovery_rows"]
        }
        overlap_ids = set(range(2, 8))
        summary["overlap_conflicts"] = 0
        for round_number in range(args.rounds):
            seed_overlap_rows(root, round_number)
            overlap_ids.update(
                round_number * 100 + key for key in range(8) if round_number * 100 + key not in {0, 1}
            )
            expected_ids.update(round_number * 100 + key for key in range(8))
            barrier = threading.Barrier(len(connections))
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(connections)) as executor:
                ids = list(
                    executor.map(
                        lambda pair: disjoint_round(pair[1], pair[0], round_number, barrier),
                        enumerate(connections),
                    )
                )
            if round_number == 0:
                summary["first_disjoint_round"] = {"rows": len(ids)}
            expected_ids.update(ids)
            fixed_values.update({row_id: f"disjoint-{round_number}-{identity}" for identity, row_id in enumerate(ids)})
            result = overlapping_round(connections, round_number)
            summary["overlap_conflicts"] += result["conflicts"]
            fixed_values.update({round_number * 100 + key: result["value"] for key in range(8)})
        summary["disjoint_merge_commits"] = args.rounds * len(connections)
        summary["overlapping_merge_commits"] = args.rounds * len(connections)
        summary["delete_recovery"] = delete_and_recover(connections[0])
        expected_ids.add(-30_000)
        fixed_values.update({0: "recovered-0", 1: "recovered-1", -30_000: "recovered-new"})
        overlap_ids.difference_update({0, 1})
        summary["final"] = verify_final_rows(connections[0], expected_ids, overlap_ids, fixed_values)
        if fetchone(connections[0], "SELECT count(*) FROM lake.events WHERE id IN (-20000, -20001)") != (1,):
            raise AssertionError("Recovery row count is incorrect")
        if args.postgres_log:
            summary["postgres_combined_query_log_matches"] = verify_postgres_log(
                args.postgres_log, metadata_schema
            )
    finally:
        for connection in connections:
            connection.close()
        with psycopg.connect(dsn, autocommit=True) as pg:
            pg.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(metadata_schema)))
        if temporary_data is not None:
            temporary_data.cleanup()
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
