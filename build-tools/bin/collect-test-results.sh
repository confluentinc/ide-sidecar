#!/bin/bash

DIR=$(dirname "$0")

if [ -d "target/test-results" ]; then
  echo "Found previously collected results from unit and integration tests in each module"
  exit 0
fi

echo "Collecting results from unit and integration tests in each module"

mkdir -p target/test-results/

# For each '${module}/target/surefire-reports' directory in each module
find . -name "surefire-reports" | while IFS= read -r dir
do
  moduleName=$(echo $dir | cut -d'/' -f2-2)
  echo "  copying unit test logs into 'target/test-results' from '$dir'"
  if [[ -d $dir ]]; then
    cp $dir/TEST-*.xml target/test-results/ 2>/dev/null
  fi
done

# For each '${module}/target/failsafe-reports' directory in each module
regex="^((target)|(pipeline-parent))$"
find . -name "failsafe-reports" | while IFS= read -r dir
do
  moduleName=$(echo $dir | cut -d'/' -f2-2)
  if ! [[ $moduleName =~ $regex ]]; then
    echo "  copying integration test logs into 'target/test-results' from '$dir'"
    if [[ -d $dir ]]; then
      cp $dir/TEST-*.xml target/test-results/ 2>/dev/null
    fi
  fi
done

echo "Collected results from unit and integration tests in each module"
