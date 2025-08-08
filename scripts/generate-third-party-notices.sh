#!/bin/bash

retry() {
  command=$1
  num_retries=$2

  for i in $(seq 1 $num_retries); do
    echo "Attempt $i: $command"
    eval $command && return
    sleep 5

    if [ $i -eq $num_retries ]; then
      echo "Failed after $num_retries attempts: $command"
      return 1
    fi
  done
}

main() {
  # Verify FOSSA CLI is available (should be installed natively in CI agent)
  command -v fossa || { echo "FOSSA CLI not found. Ensure it is installed in the CI agent."; exit 1; }

  # Full access token created using rsanjay@confluent.io's FOSSA account (on Jul 17, 2024).
  # Rotate every 80-180 days.
  # https://docs.fossa.com/docs/rotating-fossa-api-key#full-access-token
  export FOSSA_API_KEY=$(vault kv get -field api_key v1/ci/kv/fossa_full_access)
  fossa analyze --exclude-path mk-files --only-target maven

  # This might timeout so retry a few times
  retry "fossa report attribution --format text > THIRD_PARTY_NOTICES.txt" 3
}

main
