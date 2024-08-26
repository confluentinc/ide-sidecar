#!/bin/bash

DIR=$(dirname "$0")

if [ -d "target/surefire-logs" ]; then
  echo "Found previously collected log files from unit and integration tests in each module"
  exit 0
fi

echo "Collecting log files from unit and integration tests in each module"

mkdir -p target/surefire-logs/

# For each '${module}/target/surefire-reports' directory in each module
find . -name "surefire-reports" | while IFS= read -r dir
do
  moduleName=$(echo $dir | cut -d'/' -f2-2)
  echo "  copying unit test logs from '$dir' to 'target/surefire-logs/${moduleName}'"
  mkdir -p target/surefire-logs/${moduleName}
  cp $dir/* target/surefire-logs/${moduleName} 2>/dev/null
done

# For each '${module}/target/failsafe-reports' directory in each module
regex="^((target)|(pipeline-parent))$"
find . -name "failsafe-reports" | while IFS= read -r dir
do
  moduleName=$(echo $dir | cut -d'/' -f2-2)
  if ! [[ $moduleName =~ $regex ]]; then
    echo "  copying integration test logs from '$dir' to 'target/surefire-logs/${moduleName}'"
    mkdir -p target/surefire-logs/${moduleName}
    cp $dir/* target/surefire-logs/${moduleName} 2>/dev/null
  fi
done

echo "Collected log files from unit and integration tests in each module"
