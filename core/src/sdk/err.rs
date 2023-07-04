//! err contains all errors given by sdk mod.
#[derive(Debug)]
/// CustomError: I’m not quite sure what rust’s error handling should do, so I make them a String for laziness.
pub struct CustomError {
    pub origin: String,
}
impl CustomError {
    pub fn new(err: String) -> Self {
        return CustomError { origin: err };
    }
}

/// ErrorKind defined some simple error type.
/// TODO(dashjay): Makes the error more specific
#[derive(Debug)]
pub enum ErrorKind {
    IOError(CustomError),
    CloseError(CustomError),
    InvalidArgument(CustomError),
}
