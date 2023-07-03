#[derive(Debug)]
pub struct CustomError {
    pub origin: String,
}
impl CustomError {
    pub fn new(err: String) -> Self {
        return CustomError { origin: err };
    }
}

pub enum DecodeError {
    DeserializeError(CustomError),
    IOError(CustomError),
    ShortRead(CustomError),
}
