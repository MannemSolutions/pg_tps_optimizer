#!/bin/bash
set -e

docker-compose down --remove-orphans || echo new or partial install
if [ ! -e config/tls/int_server/certs/localhost.pem ]; then
  rm -rf config/tls
  docker-compose up chainsmith
fi
docker-compose up -d postgres
sleep 5
cargo run
