build:
	./set_version.sh
	cargo build

run:
	cargo run

test: unittest coverage

unittest:
	cargo test

integrationtest:
	./docker-compose-tests.sh

coverage:
	./unittests.sh

sectest: audittest

audittest:
	cargo audit

checkmate:
	cargo checkmate
