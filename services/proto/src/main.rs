use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/controller/controller.proto")?;
    Ok(())
}