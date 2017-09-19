#!/bin/bash

set -e -o pipefail

./dcos-launch describe | jq .
