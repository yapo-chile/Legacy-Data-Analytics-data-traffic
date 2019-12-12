#!/usr/bin/env bash

# Include colors.sh
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/colors.sh"

echoHeader "Deploying job definition to rundeck"

set +e

curl -X GET \
  http://3.94.225.3:4440/api/14/project/Test/jobs \
  -H "X-Rundeck-Auth-Token: ${RUNDECK_TOKEN}" \
