use std::{
    fs::File,
    io::{self},
    os::unix::fs::FileExt,
    path::PathBuf,
};

#[allow(dead_code)]
pub trait ChunkPointer {
    fn read(&self) -> io::Result<Vec<u8>>;
    fn new(path: PathBuf, offset: u64, size: usize) -> Self;
}

#[derive(Clone, Default)]
pub struct ChunkHandler {
    path: PathBuf,
    offset: u64,
    size: usize,
}

impl ChunkPointer for ChunkHandler {
    fn new(path: PathBuf, offset: u64, size: usize) -> Self {
        ChunkHandler { path, offset, size }
    }

    fn read(&self) -> io::Result<Vec<u8>> {
        let file = Result::expect(File::open(self.path.clone()), "file error");
        let mut buf = vec![0; self.size];
        let result = file.read_exact_at(&mut buf, self.offset);
        match result {
            Ok(_x) => Ok(buf),
            Err(error) => Err(error),
        }
    }
}
