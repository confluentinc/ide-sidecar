#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${DIR}/helper/functions.sh
source ${DIR}/env.sh

preflight_checks || exit
build_connect_image
create_certificates || exit 1

echo "Done creating certificates"
