#!/usr/bin/env bash
set -euo pipefail

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  echo 'This build requires Linux AMD64' >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "$script_dir/.." rev-parse --show-toplevel)
if [[ -n $(git -C "$repo_root" status --porcelain --untracked-files=all) ]]; then
  echo 'The source checkout must be clean' >&2
  exit 1
fi

workspace=$(realpath -m "${1:?Pass a new build directory}")
if [[ -e "$workspace" ]]; then
  echo 'Use a new build directory to preserve source identity' >&2
  exit 1
fi
mkdir -p "$workspace"

source_commit=$(git -C "$repo_root" rev-parse HEAD)
duckdb_revision=d8cdaa33fda8df955cc76ef58a280f68f4cd43fa
printf '%s\n' "$source_commit" > "$workspace/source-commit"
printf '%s\n' "$duckdb_revision" > "$workspace/duckdb-commit"

git init -q "$workspace/source-duckdb"
git -C "$workspace/source-duckdb" remote add origin https://github.com/duckdb/duckdb.git
git -C "$workspace/source-duckdb" fetch -q --depth 1 origin "$duckdb_revision"
git -C "$workspace/source-duckdb" checkout -q --detach FETCH_HEAD
test "$(git -C "$workspace/source-duckdb" rev-parse HEAD)" = "$duckdb_revision"

curl --fail --location --retry 3 https://codeload.github.com/RoaringBitmap/CRoaring/tar.gz/refs/tags/v4.3.12 -o "$workspace/roaring.tar.gz"
echo "9f1602d7ff83e84ce0a5928129bb957a1eeacd3b092b39e21f5f0b931682b8d6  $workspace/roaring.tar.gz" | sha256sum --check
mkdir "$workspace/roaring"
tar -xzf "$workspace/roaring.tar.gz" --strip-components=1 -C "$workspace/roaring"
cmake -S "$workspace/roaring" -B "$workspace/roaring-build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$workspace/roaring-install" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DENABLE_ROARING_TESTS=OFF \
  -DROARING_BUILD_STATIC=ON -DROARING_USE_CPM=OFF
cmake --build "$workspace/roaring-build" --parallel 4
cmake --install "$workspace/roaring-build"

mkdir "$workspace/driver" "$workspace/extensions"
curl --fail --location --retry 3 https://github.com/duckdb/duckdb/releases/download/v1.5.5/libduckdb-linux-amd64.zip -o "$workspace/driver.zip"
unzip -q "$workspace/driver.zip" -d "$workspace/driver"
echo "fc23f12e376c47be520f75221288281906e7942e8fd6f6ce4849198ba60d0405  $workspace/driver/libduckdb.so" | sha256sum --check
echo "db566c025fd9794adf98cada0b1cdf7a6e0b1389a99a1970ee940138a699d4ea  $workspace/driver/libduckdb_static.a" | sha256sum --check
for extension in httpfs postgres_scanner; do
  curl --fail --location --retry 3 "https://extensions.duckdb.org/v1.5.5/linux_amd64/$extension.duckdb_extension.gz" -o "$workspace/extensions/$extension.gz"
  gzip -dc "$workspace/extensions/$extension.gz" > "$workspace/extensions/$extension.duckdb_extension"
done
mkdir -p "$workspace/extensions/v1.5.5/linux_amd64"
cp "$workspace/extensions/"*.duckdb_extension "$workspace/extensions/v1.5.5/linux_amd64/"
sha256sum --check <<HASHES
887c392b1e49128d11667c81e3698d8b00dfdeb456771acf66d05a0f74f7b7d8  $workspace/extensions/httpfs.duckdb_extension
b1ced4cfc6311313e117c2afb3eac76508718778dde0716421503c7dbfb5605c  $workspace/extensions/postgres_scanner.duckdb_extension
HASHES

DISABLE_EXTENSIONS_FOR_TEST=1 cmake -S "$workspace/source-duckdb" -B "$workspace/build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$workspace/roaring-install" \
  -DDUCKDB_EXTENSION_CONFIGS="$repo_root/extension_config.cmake" \
  -DBUILD_UNITTESTS=OFF -DBUILD_SHELL=OFF -DENABLE_SANITIZER=OFF -DENABLE_UBSAN=OFF \
  -DEXTENSION_STATIC_BUILD=ON -DPREBUILT_BINARY="$workspace/driver/libduckdb_static.a" \
  -DOVERRIDE_GIT_DESCRIBE=v1.5.5
cmake --build "$workspace/build" --target ducklake_loadable_extension --parallel 4

sha256sum "$workspace/driver/libduckdb.so" "$workspace/driver/libduckdb_static.a" \
  "$workspace/build/extension/ducklake/ducklake.duckdb_extension" \
  "$workspace/extensions/"*.duckdb_extension > "$workspace/artifacts.sha256"
