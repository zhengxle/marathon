#!/bin/bash

set -e -o pipefail

START_DIR=$(pwd)

apk add openjdk8

wget "https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein"
chmod +x lein

cd tests/jepsen
${START_DIR}/lein deps

#./dcos-launch describe | jq .
