.PHONY: proto

proto:
	export OUT_DIR=proto/src/turbo && cargo run --bin gen-turbo-proto --manifest-path proto/Cargo.toml
	export OUT_DIR=proto/src/controller && cargo run --bin gen-controller-proto --manifest-path proto/Cargo.toml
