#!/bin/bash

set -e -o pipefail

apk add openjdk8

wget "https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein"
chmod +x lein

cd tests/jepsen
../../lein deps

#./dcos-launch describe | jq .
