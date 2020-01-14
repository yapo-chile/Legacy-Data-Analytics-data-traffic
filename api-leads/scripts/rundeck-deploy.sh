#!/usr/bin/env bash

# Include colors.sh
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/colors.sh"

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echoHeader "Deploying job definition to rundeck"

set +e

if [[ "${GIT_BRANCH}" == "master" ]]; then
    cat ${base_dir}/../deploy/api_leads.yaml | curl -X POST 'http://3.94.225.3:4440/api/14/project/data_jobs/jobs/import?fileformat=yaml&dupeOption=update&uuidOption=preserve' -H 'Content-Type: application/yaml' -H 'X-Rundeck-Auth-Token: '${RUNDECK_TOKEN}'' --data-binary '@-'
fi