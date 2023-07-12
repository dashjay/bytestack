fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/src/controller/controller.proto")?;
    tonic_build::compile_protos("proto/src/bserver/bserver.proto")?;
    Ok(())
}
