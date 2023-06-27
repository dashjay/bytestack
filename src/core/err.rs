#[derive( Debug)]
pub enum ErrorKind {
    MarshalJsonError,
    WriteError,
    None
}