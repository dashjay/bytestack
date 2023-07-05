use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("controller/controller.proto")?;
    Ok(())
}