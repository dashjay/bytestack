#[derive(Debug)]
pub enum DecodeError{
    DeserializeError,
    IOError,
    ShortRead,
}
