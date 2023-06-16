#!/usr/bin/env bash
# This script copies the jars from their built location by their specific module into a shared lib/ folder
# The corresponding extension factories, such as UnderFileSystemFactoryRegistry, will read the lib/ directory to load classes from

SCRIPT_DIR=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)
REPO_ROOT="${SCRIPT_DIR}/../.."
LIB_DIR="${REPO_ROOT}/lib"

echo "Copying extension jars..."

rm -r "${LIB_DIR}"
mkdir -p "${LIB_DIR}"

for f in "${REPO_ROOT}/dora/lib"/*.jar; do
  echo "Copying ${f}"
  cp "${f}" "${LIB_DIR}/"
done

