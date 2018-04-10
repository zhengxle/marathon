#!/bin/bash
# This script runs the scale tests on a marathon running under the specified URL

# Require a cluster URL
[ -z "$1" ] && echo -e "Usage: $0 <marathon> [<test file> ...]" && exit 1
MARATHON_URL=$1
shift

# We need the CLI, so ask the user
if ! which dcos >/dev/null; then
  echo "ERROR: Please install the DC/OS CLI before using this tool"
  exit 1
fi
if ! which dcos-perf-test-driver >/dev/null; then
  echo "ERROR: Please install the DC/OS Performance Test Driver before using this tool"
  exit 1
fi

# Collect some useful metadata
GIT_VERSION=$(git rev-parse HEAD)

# If the user hasn't specified any test files, run them all
TESTS=$*
[ -z "$TESTS" ] && TESTS=$(ls config/perf-driver/test-*.yml)
for TEST in $TESTS; do
  dcos-perf-test-driver \
    ./config/perf-driver/environments/target-standalone.yml \
    ./config/perf-driver/environments/env-local.yml \
    "./$TEST" \
    -M "version=None" \
    -M "marathon=git-${GIT_VERSION}" \
    -D "marathon_url=${MARATHON_URL}" \
    --verbose
done
