build:
	cargo build

run:
	cargo run

test: coverage

unittest:
	cargo test

coverage:
	./unittests.sh
