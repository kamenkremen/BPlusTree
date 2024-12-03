use std::{fs::File, io::{self, Read, Write}};
#[allow(dead_code)]
pub trait ChunkPointer {
    fn write(&self, content: &[u8]) -> Result<(), std::io::Error>;
    fn read(&self, buf: &mut Vec<u8>) -> io::Result<usize>;
    fn new(path: String) -> Self;
}

#[derive(Clone)]
pub struct FileHandler {pub path: String}

impl FileHandler {
    pub fn default() -> Self {
        FileHandler{path: "default".to_string()}
    }
}

impl ChunkPointer for FileHandler {
    fn new(path: String) -> Self {
        FileHandler {path: path}
    }

    fn write(&self, content: &[u8]) -> Result<(), std::io::Error> {
        let mut file = File::create(self.path.clone()).expect("error in file creation");
        file.write_all(content)
    }

    fn read(&self, buf:  &mut Vec<u8>) -> io::Result<usize> {
        let mut file = Result::expect(File::open(self.path.clone()), "file does not exist");
        file.read_to_end(buf)
    }
}