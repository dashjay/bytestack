pub struct FileLike<T>
where
    T: std::io::Read + std::io::Write + std::io::Seek,
{
    fd: T,
}

impl<T> FileLike<T>
where
    T: std::io::Read + std::io::Write + std::io::Seek,
{
    fn new(fl: T) -> FileLike<T> {
        return FileLike { fd: fl };
    }
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        return self.fd.read(buf);
    }
    fn write(&mut self, buf: &[u8])-> Result<usize, std::io::Error>{
        return self.fd.write(buf);
    }
    fn seek(&mut self, pos: std::io::SeekFrom)->Result<u64, std::io::Error>{
        return self.fd.seek(pos);
    }
}
