#!/bin/bash

# Note: This script must be run from the root directory of the repository

# Set CLEAN to true to re-clone the cp-demo directory and re-create the certificates
CLEAN=${CLEAN:-false}

# Ensure yq is installed before running this script
if ! command -v yq &> /dev/null; then
  echo "yq is not installed. Please install yq before running this script."
  echo "If you are on macOS, you can install yq using brew: brew install yq"
  exit 1
fi

# Read test.cp-demo.tag from application.yml using yq
CP_DEMO_TAG=$(yq e '.ide-sidecar.integration-test-configs.cp-demo.tag' src/main/resources/application.yml)

CP_DEMO_DIR="$(pwd)/.cp-demo"
CP_DEMO_SCRIPTS_DIR="${CP_DEMO_DIR}/scripts"

clone_cp_demo_if_not_exists() {
  # Delete cp-demo if CLEAN is set to true
  if [ "${CLEAN}" == "true" ]; then
    echo "CLEAN is set to true. Deleting cp-demo directory."
    rm -rf ${CP_DEMO_DIR}
  fi

  if [ ! -d "${CP_DEMO_DIR}" ]; then
    echo "Cloning cp-demo (tag: ${CP_DEMO_TAG}) to ${CP_DEMO_DIR}"
    git clone --branch ${CP_DEMO_TAG} https://github.com/confluentinc/cp-demo.git ${CP_DEMO_DIR} \
      --depth 1 \
      --quiet

    if [ $? -ne 0 ]; then
      echo "Failed to clone cp-demo"
      exit 1
    fi

    echo "Cloned cp-demo successfully. "
    echo "Note: If you manually modify the contents of the cp-demo directory, "
    echo "you may experience issues with the CP integration tests"
  else
    echo "cp-demo already exists at ${CP_DEMO_DIR}. Skipping cloning."
  fi
}

# Overrides create_certificates from functions.sh
create_certificates()
{
  # If clean is set, remove the .certs-created file
  if [ "${CLEAN}" == "true" ]; then
    echo "CLEAN is set to true. Deleting .certs-created file."
    rm -f ${CP_DEMO_SCRIPTS_DIR}/security/.certs-created
  fi

  # Check if certificates have already been created
  if [ -f ${CP_DEMO_SCRIPTS_DIR}/security/.certs-created ]; then
    echo "Certificates have already been created. Skipping certificate creation."
    return
  fi
  # Generate keys and certificates used for SSL
  echo -e "Generate keys and certificates used for SSL (see ${CP_DEMO_SCRIPTS_DIR}/security)"
  # Install findutils to be able to use 'xargs' in the certs-create.sh script
  docker run -v ${CP_DEMO_SCRIPTS_DIR}/security/:/etc/kafka/secrets/ -u0 $REPOSITORY/cp-server:${CONFLUENT_DOCKER_TAG} bash -c "yum -y install findutils; cd /etc/kafka/secrets && ./certs-create.sh && chown -R $(id -u $USER):$(id -g $USER) /etc/kafka/secrets"

  # Generating public and private keys for token signing
  echo "Generating public and private keys for token signing"
  docker run -v ${CP_DEMO_SCRIPTS_DIR}/security/:/etc/kafka/secrets/ -u0 $REPOSITORY/cp-server:${CONFLUENT_DOCKER_TAG} bash -c "mkdir -p /etc/kafka/secrets/keypair; openssl genrsa -out /etc/kafka/secrets/keypair/keypair.pem 2048; openssl rsa -in /etc/kafka/secrets/keypair/keypair.pem -outform PEM -pubout -out /etc/kafka/secrets/keypair/public.pem && chown -R $(id -u $USER):$(id -g $USER) /etc/kafka/secrets/keypair"

  # Enable Docker appuser to read files when created by a different UID
  echo -e "Setting insecure permissions on some files in ${CP_DEMO_SCRIPTS_DIR}/security for demo purposes\n"
  chmod 644 ${CP_DEMO_SCRIPTS_DIR}/security/keypair/keypair.pem
  chmod 644 ${CP_DEMO_SCRIPTS_DIR}/security/*.key
  if [ $? -ne 0 ]; then
    echo "Failed to set insecure permissions on some files in ${CP_DEMO_SCRIPTS_DIR}/security"
    exit 1
  fi
  echo "Done creating certificates"

  # Throw a file in there to indicate that the certs have been created
  touch ${CP_DEMO_SCRIPTS_DIR}/security/.certs-created
}

update_cp_demo_to_match_integration_test_setup() {
  # Override the broker_jaas.conf file in cp-demo with the one in the test resources
  cp src/test/resources/cp-demo-scripts/broker_jaas.conf ${CP_DEMO_SCRIPTS_DIR}/security/broker_jaas.conf
}

main() {
  clone_cp_demo_if_not_exists || exit 1
  source ${CP_DEMO_SCRIPTS_DIR}/env.sh
  create_certificates || exit 1
  update_cp_demo_to_match_integration_test_setup || exit 1

  echo "✅ cp-demo setup completed successfully."
}

main
