#!/usr/bin/env bash
#
# Regenerate the Thrift-generated Java sources under
# jdbc-core/src/main/java/com/databricks/jdbc/model/client/thrift/generated/
# from the IDL at
# jdbc-core/src/main/java/com/databricks/jdbc/dbclient/impl/thrift/TCLIService.thrift.
#
# Why this exists: the project ships a snapshot of the Thrift-generated
# Java code (~107 files, ~30k LOC). Each time we bump <thrift.version>
# in pom.xml across a major-ish boundary (e.g. 0.19 -> 0.21+), the
# Thrift runtime's generic signatures change and the checked-in
# generated code fails to compile until it's regenerated with the
# matching compiler version. Before this script, that regeneration
# was tribal knowledge.
#
# Usage:
#   ./scripts/regenerate-thrift.sh
#
# What it does:
#   1. Builds the Apache Thrift compiler from source (the official
#      Docker images aren't versioned past 0.12; building takes <1 min
#      with the libs disabled). The compiler is built into a temporary
#      dir and not installed system-wide.
#   2. Runs `thrift --gen java` against the IDL.
#   3. Replaces the contents of the generated/ directory.
#   4. Runs `mvn spotless:apply` so the new sources match the
#      project's formatting (Thrift's raw output has long lines and
#      different brace style).
#
# Source of truth for the IDL: ~/universe/peco/thrift/TCLIService.thrift
# (the same IDL feeds the Python, Node.js, Go, and Java SQL drivers --
# see ~/universe/peco/thrift/README.md). If you need to update the IDL,
# do it in that repo first, then re-sync into jdbc-core via this script
# before regenerating.

set -euo pipefail

# Match this to the version pinned in pom.xml's <thrift.version>.
THRIFT_VERSION="${THRIFT_VERSION:-0.23.0}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IDL="${REPO_ROOT}/jdbc-core/src/main/java/com/databricks/jdbc/dbclient/impl/thrift/TCLIService.thrift"
GENDIR="${REPO_ROOT}/jdbc-core/src/main/java/com/databricks/jdbc/model/client/thrift/generated"
BUILD_DIR="$(mktemp -d -t thrift-build-XXXXXX)"
OUT_DIR="$(mktemp -d -t thrift-gen-XXXXXX)"

trap 'rm -rf "${BUILD_DIR}" "${OUT_DIR}"' EXIT

if [ ! -f "${IDL}" ]; then
  echo "ERROR: IDL file not found at ${IDL}" >&2
  exit 1
fi

# --- 1. Build the Thrift compiler ---------------------------------------
echo "==> Building Thrift compiler ${THRIFT_VERSION} (this takes ~1 minute)"

curl -fsSL -o "${BUILD_DIR}/thrift.tar.gz" \
  "https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"

tar -xzf "${BUILD_DIR}/thrift.tar.gz" -C "${BUILD_DIR}"
cd "${BUILD_DIR}/thrift-${THRIFT_VERSION}"

# Disable language libraries (we only need the compiler binary) so the
# build doesn't fail on missing per-language toolchains.
./configure \
  --enable-libs=no \
  --disable-tests \
  --disable-tutorial \
  --prefix="${BUILD_DIR}/install" \
  >/dev/null

make -j"$(nproc)" >/dev/null

THRIFT_BIN="${BUILD_DIR}/thrift-${THRIFT_VERSION}/compiler/cpp/thrift"
"${THRIFT_BIN}" --version

# --- 2. Run codegen ------------------------------------------------------
echo "==> Generating Java sources to ${OUT_DIR}"
"${THRIFT_BIN}" --gen java -out "${OUT_DIR}" "${IDL}"

GEN_PKG="${OUT_DIR}/com/databricks/jdbc/model/client/thrift/generated"
if [ ! -d "${GEN_PKG}" ]; then
  echo "ERROR: expected ${GEN_PKG} to exist after codegen but it does not." >&2
  exit 1
fi

NEW_COUNT=$(find "${GEN_PKG}" -name '*.java' | wc -l)
echo "    generated ${NEW_COUNT} Java files"

# --- 3. Replace the generated/ dir --------------------------------------
echo "==> Replacing ${GENDIR}"
rm -f "${GENDIR}"/*.java
cp "${GEN_PKG}"/*.java "${GENDIR}"/

# --- 4. Apply spotless formatting ---------------------------------------
echo "==> Running spotless:apply"
cd "${REPO_ROOT}"
mvn -pl jdbc-core spotless:apply --batch-mode -q

echo ""
echo "Done. Run \`git diff\` to inspect the result, then \`mvn clean install\` to verify."
