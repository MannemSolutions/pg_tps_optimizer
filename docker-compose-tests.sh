#!/bin/bash
set -ex

docker-compose down --remove-orphans || echo new or partial install
docker rmi pg_tps_optimizer-pg_tps_optimizer || echo image was not there
if [ ! -e config/tls/int_server/certs/localhost.pem ]; then
  rm -rf config/tls
  chmod 777 config
  docker-compose up chainsmith
fi
docker-compose up -d postgres

for ((i=0;i<60;i++)); do
  docker-compose exec -u postgres postgres pg_isready && break
  sleep 1
done

cargo test -- --include-ignored
docker-compose up pg_tps_optimizer
