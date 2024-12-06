#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
source ${DIR}/helper/functions.sh
source ${DIR}/env.sh

preflight_checks || exit

CLEAN=${CLEAN:-false}

echo
echo "Environment parameters"
echo "  REPOSITORY=$REPOSITORY"
echo "  CONNECTOR_VERSION=$CONNECTOR_VERSION"
echo "  CLEAN=$CLEAN"
echo "  VIZ=$VIZ"
echo "  C3_KSQLDB_HTTPS=$C3_KSQLDB_HTTPS"
echo


if [[ "$CLEAN" == "true" ]] ; then
  create_certificates || exit 1
fi

# Bring up tools
docker-compose up --no-recreate -d tools

# Add root CA to container (obviates need for supplying it at CLI login '--ca-cert-path')
docker-compose exec tools bash -c "cp /etc/kafka/secrets/snakeoil-ca-1.crt /usr/local/share/ca-certificates && /usr/sbin/update-ca-certificates"

echo "Creating role bindings for principals"
docker-compose exec tools bash -c "/tmp/helper/create-role-bindings.sh" || exit 1

echo -e "\nAvailable LDAP users:"
#docker-compose exec openldap ldapsearch -x -h localhost -b dc=confluentdemo,dc=io -D "cn=admin,dc=confluentdemo,dc=io" -w admin | grep uid:
curl -u mds:mds -X POST "https://localhost:8091/security/1.0/principals/User%3Amds/roles/UserAdmin" \
  -H "accept: application/json" -H "Content-Type: application/json" \
  -d "{\"clusters\":{\"kafka-cluster\":\"does_not_matter\"}}" \
  --cacert ${DIR}/security/snakeoil-ca-1.crt --tlsv1.2
curl -u mds:mds -X POST "https://localhost:8091/security/1.0/rbac/principals" --silent \
  -H "accept: application/json"  -H "Content-Type: application/json" \
  -d "{\"clusters\":{\"kafka-cluster\":\"does_not_matter\"}}" \
  --cacert ${DIR}/security/snakeoil-ca-1.crt --tlsv1.2 | jq '.[]'

poststart_checks

