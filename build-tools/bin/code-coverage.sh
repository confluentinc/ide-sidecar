#!/bin/bash

python3 -c "from bs4 import BeautifulSoup"
if [[ $? -eq 1 ]]
then
  sudo apt install -y python3-bs4
fi

DIR=$(dirname "$0")
PERCENT=$(${DIR}/code-coverage.py)

echo "Code Coverage is at: ${PERCENT}"

sed s/######/${PERCENT}/g ${DIR}/../coverage/coverage.svg.template > coverage.svg
cat coverage.svg
