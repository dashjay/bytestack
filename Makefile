.PHONY: proto

proto:
	export OUT_DIR=proto/src/controller && cargo run --manifest-path proto/Cargo.toml
