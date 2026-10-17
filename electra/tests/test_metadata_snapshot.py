import argparse
import concurrent.futures
import json
import tempfile
import threading
import uuid
from collections import Counter
from pathlib import Path

import duckdb
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--extension", type=Path, required=True)
    parser.add_argument("--extension-cache", type=Path, required=True)
    parser.add_argument("--checks", type=int, default=500)
    parser.add_argument("--writes", type=int, default=300)
    args = parser.parse_args()
    options = conninfo_to_dict(args.dsn)
    if options.keys() - {"host", "port", "dbname", "user", "password"}:
        parser.error("Only host, port, dbname, user, and password are accepted")
    if (
        options.get("host") != "127.0.0.1"
        or options.get("port") != "15545"
        or options.get("dbname") != "lake_writer_repro"
    ):
        parser.error("Use the disposable database lake_writer_repro at 127.0.0.1:15545")
    args.dsn = make_conninfo(**options, hostaddr="127.0.0.1")
    if args.checks < 1 or args.writes < 1:
        parser.error("Checks and writes must be positive")
    schema = "snapshot_probe_" + uuid.uuid4().hex
    lake_schema = "lake_probe_" + uuid.uuid4().hex + "'quoted"
    summary = {}

    def connect():
        client = duckdb.connect(config={"allow_unsigned_extensions": True})
        client.execute("SET threads = 2")
        client.execute(f"SET extension_directory = {literal(args.extension_cache.resolve())}")
        client.execute("LOAD httpfs; LOAD postgres")
        client.execute(f"LOAD {literal(args.extension.resolve())}")
        return client

    with psycopg.connect(args.dsn, autocommit=True) as pg, tempfile.TemporaryDirectory() as data:
        if pg.execute("SELECT current_database()").fetchone()[0] != "lake_writer_repro":
            raise RuntimeError("Unexpected database")
        try:
            pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            pg.execute(sql.SQL("CREATE TABLE {}.snapshots (snapshot_id BIGINT PRIMARY KEY)").format(sql.Identifier(schema)))
            pg.execute(sql.SQL("INSERT INTO {}.snapshots SELECT generate_series(0, 100000)").format(sql.Identifier(schema)))
            client = connect()
            try:
                client.execute(f"ATTACH {literal(args.dsn)} AS metadata (TYPE postgres, ISOLATION_LEVEL 'READ COMMITTED')")
                summary["metadata_reads"] = check_reads(client, args, schema)
            finally:
                client.close()

            attach = (
                f"ATTACH {literal('ducklake:postgres:' + args.dsn)} AS lake "
                f"(METADATA_SCHEMA {literal(lake_schema)}, DATA_PATH {literal(data)})"
            )
            client = connect()
            try:
                client.execute(attach)
                client.execute("CREATE TABLE lake.events (writer INTEGER, sequence INTEGER)")
                summary["commits"] = check_commits(connect, attach, client, args.writes)
                summary["missing_snapshot_guard"] = check_missing_snapshot(
                    connect, attach, client, pg, lake_schema
                )
                summary["empty_result_guard"] = check_missing_snapshot(
                    connect, attach, client, pg, lake_schema, empty=True
                )
            finally:
                client.close()
        finally:
            for name in [schema, lake_schema]:
                pg.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))
    print(json.dumps(summary, indent=2))


def check_reads(client, args, schema):
    stop = threading.Event()
    started = threading.Event()

    def writer():
        with psycopg.connect(args.dsn, autocommit=True) as pg:
            snapshot = pg.execute(
                sql.SQL("SELECT COALESCE(MAX(snapshot_id), -1) + 1 FROM {}.snapshots").format(
                    sql.Identifier(schema)
                )
            ).fetchone()[0]
            initial_snapshot = snapshot
            started.set()
            while not stop.is_set():
                pg.execute(sql.SQL("INSERT INTO {}.snapshots VALUES (%s)").format(sql.Identifier(schema)), [snapshot])
                snapshot += 1
        return snapshot - initial_snapshot

    remote = f'SELECT snapshot_id FROM "{schema}".snapshots WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM "{schema}".snapshots)'
    split = remote.replace(f'"{schema}".', f'metadata."{schema}".')
    atomic = f"SELECT * FROM postgres_query('metadata', {literal(remote)})"
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        work = executor.submit(writer)
        try:
            if not started.wait(timeout=30):
                raise AssertionError("Concurrent metadata writer did not start")
            for name, query in [("split", split), ("atomic", atomic)]:
                missing = sum(len(client.execute(query).fetchall()) != 1 for _ in range(args.checks))
                results[name] = {"checks": args.checks, "missing_snapshot": missing}
        finally:
            stop.set()
        results["concurrent_commits"] = work.result(timeout=30)
    if results["concurrent_commits"] < 1:
        raise AssertionError(
            {"reason": "No concurrent PostgreSQL commits were observed", **results}
        )
    if results["split"]["missing_snapshot"] < 1:
        raise AssertionError(
            {
                "reason": "The split metadata read observed no race; the test is inconclusive",
                **results,
            }
        )
    if results["atomic"]["missing_snapshot"]:
        raise AssertionError(results)
    return results


def check_commits(connect, attach, observer, writes):
    barrier = threading.Barrier(4)

    def writer(identity):
        client = connect()
        try:
            client.execute(attach)
            client.execute("SET ducklake_max_retry_count = 100")
            barrier.wait(timeout=30)
            for sequence in range(writes):
                client.execute("INSERT INTO lake.events VALUES (?, ?)", [identity, sequence])
            if client.execute("SELECT 42").fetchone() != (42,):
                raise AssertionError("Connection is unusable after commits")
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(writer, range(4)))
    rows, distinct_rows = observer.execute(
        "SELECT count(*), count(DISTINCT (writer, sequence)) FROM lake.events"
    ).fetchone()
    if (rows, distinct_rows) != (4 * writes, 4 * writes):
        raise AssertionError({"rows": rows, "distinct_rows": distinct_rows})
    return {"writers": 4, "rows": rows, "distinct_rows": distinct_rows}


def check_missing_snapshot(connect, attach, observer, pg, schema, empty=False):
    table = "empty_guard_events" if empty else "guard_events"
    observer.execute(f"CREATE TABLE lake.{table} (writer INTEGER, sequence INTEGER)")
    observer.execute(f"INSERT INTO lake.{table} VALUES (-1, -1)")
    pg.execute(
        sql.SQL("CREATE TABLE {}.fault_switch (enabled BOOLEAN NOT NULL)").format(sql.Identifier(schema))
    )
    pg.execute(sql.SQL("INSERT INTO {}.fault_switch VALUES (true)").format(sql.Identifier(schema)))
    pg.execute(sql.SQL("ALTER TABLE {}.ducklake_snapshot RENAME TO snapshot_rows").format(sql.Identifier(schema)))
    pg.execute(sql.SQL(
        "CREATE VIEW {}.ducklake_snapshot AS SELECT * FROM {}.snapshot_rows "
        "WHERE strpos(current_query(), 'UNION ALL') = 0 "
        "OR NOT (SELECT enabled FROM {}.fault_switch)"
    ).format(sql.Identifier(schema), sql.Identifier(schema), sql.Identifier(schema)))
    if empty:
        pg.execute(sql.SQL("ALTER TABLE {}.ducklake_table_stats RENAME TO stats_rows").format(sql.Identifier(schema)))
        pg.execute(sql.SQL(
            "CREATE VIEW {}.ducklake_table_stats AS SELECT * FROM {}.stats_rows "
            "WHERE strpos(current_query(), 'UNION ALL') = 0 "
            "OR NOT (SELECT enabled FROM {}.fault_switch)"
        ).format(sql.Identifier(schema), sql.Identifier(schema), sql.Identifier(schema)))
    clients = []
    barrier = threading.Barrier(2)
    views_restored = False
    fault_enabled = True

    def disable_fault():
        nonlocal fault_enabled
        if not fault_enabled:
            return
        pg.execute(sql.SQL("UPDATE {}.fault_switch SET enabled = false").format(sql.Identifier(schema)))
        fault_enabled = False

    def restore_views():
        nonlocal views_restored
        if views_restored:
            return
        if empty:
            pg.execute(sql.SQL("DROP VIEW {}.ducklake_table_stats").format(sql.Identifier(schema)))
            pg.execute(
                sql.SQL("ALTER TABLE {}.stats_rows RENAME TO ducklake_table_stats").format(
                    sql.Identifier(schema)
                )
            )
        pg.execute(sql.SQL("DROP VIEW {}.ducklake_snapshot").format(sql.Identifier(schema)))
        pg.execute(
            sql.SQL("ALTER TABLE {}.snapshot_rows RENAME TO ducklake_snapshot").format(
                sql.Identifier(schema)
            )
        )
        views_restored = True

    def writer(identity, client):
        failed_rows = []
        successful_rows = []
        round_outcomes = []
        failures = 0
        for sequence in range(30):
            row = (identity, sequence)
            client.execute("BEGIN")
            client.execute(f"INSERT INTO lake.{table} VALUES (?, ?)", list(row))
            barrier.wait(timeout=30)
            try:
                client.execute("COMMIT")
            except duckdb.TransactionException as error:
                if "current DuckLake snapshot is missing" not in str(error):
                    raise
                failures += 1
                failed_rows.append(row)
                if client.execute("SELECT 42").fetchone() != (42,):
                    raise AssertionError("Database invalidated after a missing snapshot")
                if client.execute(
                    f"SELECT count(*) FROM lake.{table} WHERE writer = ? AND sequence = ?", list(row)
                ).fetchone() != (0,):
                    raise AssertionError({"reason": "Failed row remained visible", "row": row})
                round_outcomes.append((sequence, "failed", row))
            else:
                successful_rows.append(row)
                round_outcomes.append((sequence, "successful", row))
                if client.execute(
                    f"SELECT count(*) FROM lake.{table} WHERE writer = ? AND sequence = ?", list(row)
                ).fetchone() != (1,):
                    raise AssertionError({"reason": "Successful row was not committed", "row": row})
            barrier.wait(timeout=30)
        return {
            "failures": failures,
            "failed_rows": failed_rows,
            "successful_rows": successful_rows,
            "round_outcomes": round_outcomes,
        }

    try:
        try:
            clients = [connect() for _ in range(2)]
            for client in clients:
                client.execute(attach)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = list(executor.map(writer, range(2), clients))
            failures = sum(outcome["failures"] for outcome in outcomes)
            if failures != 30:
                raise AssertionError(
                    {
                        "reason": "The missing-snapshot guard did not reject one commit per round",
                        "outcomes": outcomes,
                    }
                )
            rounds = {}
            for outcome in outcomes:
                for sequence, status, row in outcome["round_outcomes"]:
                    rounds.setdefault(sequence, []).append((status, row))
            if set(rounds) != set(range(30)) or any(
                Counter(status for status, _ in round_rows) != Counter({"failed": 1, "successful": 1})
                for round_rows in rounds.values()
            ):
                raise AssertionError({"reason": "Each round must have one failed and one successful commit", "outcomes": outcomes})
            expected_rows = [(-1, -1)]
            expected_rows.extend(row for outcome in outcomes for row in outcome["successful_rows"])
            observed_rows = observer.execute(f"SELECT writer, sequence FROM lake.{table}").fetchall()
            if Counter(observed_rows) != Counter(expected_rows):
                raise AssertionError(
                    {
                        "reason": "The guard left an unexpected row set",
                        "expected_rows": expected_rows,
                        "observed_rows": observed_rows,
                    }
                )
            if not failures:
                raise AssertionError("The missing-snapshot failure path was not exercised")
        finally:
            disable_fault()
        recovery_rows = [(identity, 1000) for identity in range(len(clients))]
        for client, row in zip(clients, recovery_rows):
            client.execute(f"INSERT INTO lake.{table} VALUES (?, ?)", list(row))
            if client.execute(
                f"SELECT count(*) FROM lake.{table} WHERE writer = ? AND sequence = ?", list(row)
            ).fetchone() != (1,):
                raise AssertionError({"reason": "Same connection could not write after recovery", "row": row})
        expected_rows.extend(recovery_rows)
        observed_rows = observer.execute(f"SELECT writer, sequence FROM lake.{table}").fetchall()
        if Counter(observed_rows) != Counter(expected_rows):
            raise AssertionError(
                {
                    "reason": "Recovery writes did not produce the exact expected rows",
                    "expected_rows": expected_rows,
                    "observed_rows": observed_rows,
                }
            )
        return {
            "recoverable_errors": failures,
            "failed_rows": failures,
            "successful_rows": len(expected_rows) - 1,
            "recovery_rows": len(recovery_rows),
            "empty_result": empty,
            "database_remained_usable": True,
        }
    finally:
        try:
            for client in clients:
                client.close()
        finally:
            try:
                pg.execute("SET lock_timeout = '5s'")
                restore_views()
            finally:
                pg.execute(sql.SQL("DROP TABLE IF EXISTS {}.fault_switch CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()
