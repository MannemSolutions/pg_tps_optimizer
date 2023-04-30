#!/bin/bash
set -e

docker-compose down --remove-orphans || echo new or partial install
if [ ! -e config/tls/int_server/certs/localhost.pem ]; then
  rm -rf config/tls
  docker-compose up chainsmith
fi
docker-compose up -d postgres
sleep 5
export PGSSLCERT=config/tls/int_client/certs/postgres.pem
export PGSSLKEY=config/tls/int_client/private/postgres.key.pem
export PGSSLROOTCERT=config/tls/int_server/certs/ca-chain-bundle.cert.pem
cargo run
