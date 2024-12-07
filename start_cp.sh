#!/bin/bash

# Note: This script must be run from the root directory of the repository
DIR="$(pwd)/.cp-demo/scripts"
source ${DIR}/helper/functions.sh
source ${DIR}/env.sh

# Overrides create_certificates from functions.sh
create_certificates()
{
  # Generate keys and certificates used for SSL
  echo -e "Generate keys and certificates used for SSL (see ${DIR}/security)"
  # Install findutils to be able to use 'xargs' in the certs-create.sh script
  docker run -v ${DIR}/security/:/etc/kafka/secrets/ -u0 $REPOSITORY/cp-server:${CONFLUENT_DOCKER_TAG} bash -c "yum -y install findutils; cd /etc/kafka/secrets && ./certs-create.sh && chown -R $(id -u $USER):$(id -g $USER) /etc/kafka/secrets"

  # Generating public and private keys for token signing
  echo "Generating public and private keys for token signing"
  docker run -v ${DIR}/security/:/etc/kafka/secrets/ -u0 $REPOSITORY/cp-server:${CONFLUENT_DOCKER_TAG} bash -c "mkdir -p /etc/kafka/secrets/keypair; openssl genrsa -out /etc/kafka/secrets/keypair/keypair.pem 2048; openssl rsa -in /etc/kafka/secrets/keypair/keypair.pem -outform PEM -pubout -out /etc/kafka/secrets/keypair/public.pem && chown -R $(id -u $USER):$(id -g $USER) /etc/kafka/secrets/keypair"

  # Enable Docker appuser to read files when created by a different UID
  echo -e "Setting insecure permissions on some files in ${DIR}/security for demo purposes\n"
  chmod 644 ${DIR}/security/keypair/keypair.pem
  chmod 644 ${DIR}/security/*.key
}

preflight_checks || exit
create_certificates || exit 1

echo "Done creating certificates"
