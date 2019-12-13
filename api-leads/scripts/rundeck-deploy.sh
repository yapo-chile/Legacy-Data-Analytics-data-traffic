#!/usr/bin/env bash

# Include colors.sh
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/colors.sh"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echoHeader "Deploying job definition to rundeck"

set +e

curl -X POST \
  http://3.94.225.3:4440/api/14/project/data_jobs/import?dupeOption=update&uuidOption=preserve \
  -H "Content-Type: application/yaml" \
  -H "X-Rundeck-Auth-Token: ${RUNDECK_TOKEN}" \
  -d "@${base_dir}/../deploy/api_leads.yaml"
