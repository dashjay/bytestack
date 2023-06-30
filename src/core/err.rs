#[derive( Debug)]
pub struct CustomError{
    pub origin: String
}
impl CustomError {
    pub fn new(err: String)->Self{
        return CustomError { origin: err }
    }
}


#[derive(Debug)]
pub enum ErrorKind {
    WriteError(CustomError),
    ReadError(CustomError)
}
