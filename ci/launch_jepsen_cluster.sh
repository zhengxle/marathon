#!/bin/bash

set -e -o pipefail

# One parameter is expected: CHANNEL which is a pull request for which to run Jepsen tests.
if [ "$#" -ne 1 ]; then
    echo "Expected 2 parameters: <channel> and <variant> e.g. launch_jepsen_cluster.sh testing/pull/1739"
    exit 1
fi

CHANNEL="$1"

JOB_NAME_SANITIZED=$(echo "$JOB_NAME" | tr -c '[:alnum:]-' '-')
DEPLOYMENT_NAME="$JOB_NAME_SANITIZED-$(date +%s)"

TEMPLATE="https://s3.amazonaws.com/downloads.dcos.io/dcos/${CHANNEL}/cloudformation/multi-master.cloudformation.json"

echo "Workspace: ${WORKSPACE}"
echo "Using: ${TEMPLATE}"

apk update
apk --upgrade add gettext wget
wget "https://downloads.dcos.io/dcos-test-utils/bin/linux/dcos-launch" && chmod +x dcos-launch


envsubst <<EOF > config.yaml
---
launch_config_version: 1
template_url: $TEMPLATE
deployment_name: $DEPLOYMENT_NAME
provider: aws
aws_region: us-west-2
template_parameters:
    KeyName: default
    AdminLocation: 0.0.0.0/0
    PublicSlaveInstanceCount: 0
    SlaveInstanceCount: 3
EOF

function create-junit-xml {
    local testsuite_name=$1
    local testcase_name=$2
    local error_message=$3

	cat > report.xml <<-EOF
	<testsuites>
	  <testsuite name="$testsuite_name" errors="0" skipped="0" tests="1" failures="1">
	      <testcase classname="$testsuite_name" name="$testcase_name">
	        <failure message="test setup failed">$error_message</failure>
	      </testcase>
	  </testsuite>
	</testsuites>
	EOF
}

#if ! ./dcos-launch create; then
#  create-junit-xml "dcos-launch" "cluster.create" "Cluster launch failed."
#  exit 1
#fi
#if ! ./dcos-launch wait; then
#  create-junit-xml "dcos-launch" "cluster.create" "Cluster did not start in time."
#  exit 1
#fi

# Return dcos_url
#echo "http://$(./dcos-launch describe | jq -r ".masters[0].public_ip")/"
#echo "http://$(./dcos-launch describe | jq -r ".masters[1].public_ip")/"
#echo "http://$(./dcos-launch describe | jq -r ".masters[2].public_ip")/"
