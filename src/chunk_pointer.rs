use std::{
    fs::{File, OpenOptions},
    io::{self},
    os::unix::fs::FileExt,
    path::PathBuf,
};

#[allow(dead_code)]
pub trait ChunkPointer {
    fn read(&self) -> io::Result<Vec<u8>>;
    fn new(path: PathBuf, offset: u64, content: Vec<u8>, is_new: bool) -> Self;
}

#[derive(Clone, Default)]
pub struct ChunkHandler {
    path: PathBuf,
    offset: u64,
    size: usize,
}

impl ChunkPointer for ChunkHandler {
    fn new(path: PathBuf, offset: u64, content: Vec<u8>, is_new: bool) -> Self {
        let file;

        if path.exists() && !is_new {
            file = Result::expect(
                OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(path.clone()),
                "file reading error",
            );
        } else {
            file = File::create(path.clone()).expect("error in file creation");
        }

        let size = content.len();

        let res = file.write_at(&content, offset);
        match res {
            Err(error) => panic!("{}", error),
            Ok(_num) => ChunkHandler {
                path: path,
                offset: offset,
                size: size,
            },
        }
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
