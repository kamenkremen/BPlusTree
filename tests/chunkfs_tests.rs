extern crate chunkfs;

use std::collections::HashMap;
use std::io;
use std::io::{Seek, Write};
use std::path::PathBuf;

use approx::assert_relative_eq;

use bplus_tree::bplus_tree::BPlus;
use chunkfs::chunkers::{FSChunker, LeapChunker};
use chunkfs::hashers::SimpleHasher;
use chunkfs::{create_cdc_filesystem, DataContainer, Database, WriteMeasurements};
use tempdir::TempDir;

const MB: usize = 1024 * 1024;

#[test]
fn write_read_complete_test() {
    let tempdir = &TempDir::new("storage1").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);

    let mut handle = fs.create_file("file", LeapChunker::default()).unwrap();
    fs.write_to_file(&mut handle, &[1; MB]).unwrap();
    fs.write_to_file(&mut handle, &[1; MB]).unwrap();

    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    let read = fs.read_file_complete(&handle).unwrap();
    assert_eq!(read.len(), MB * 2);
    assert_eq!(read, [1; MB * 2]);
}

#[test]
fn write_read_blocks_test() {
    let tempdir = &TempDir::new("storage2").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let ones = vec![1; MB];
    let twos = vec![2; MB];
    let threes = vec![3; MB];
    fs.write_to_file(&mut handle, &ones).unwrap();
    fs.write_to_file(&mut handle, &twos).unwrap();
    fs.write_to_file(&mut handle, &threes).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), twos);
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), threes);
}

#[test]
fn read_file_with_size_less_than_1mb() {
    let tempdir = &TempDir::new("storage3").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let ones = vec![1; 10];
    fs.write_to_file(&mut handle, &ones).unwrap();
    let measurements = fs.close_file(handle).unwrap();
    println!("{:?}", measurements);

    let mut handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_from_file(&mut handle).unwrap(), ones);
}

#[test]
fn write_read_big_file_at_once() {
    let tempdir = &TempDir::new("storage4").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let data = vec![1; 3 * MB + 50];
    fs.write_to_file(&mut handle, &data).unwrap();
    fs.close_file(handle).unwrap();

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_file_complete(&handle).unwrap().len(), data.len());
}

#[test]
fn two_file_handles_to_one_file() {
    let tempdir = &TempDir::new("storage6").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);
    let mut handle1 = fs.create_file("file", LeapChunker::default()).unwrap();
    let mut handle2 = fs.open_file("file", LeapChunker::default()).unwrap();
    fs.write_to_file(&mut handle1, &[1; MB]).unwrap();
    fs.close_file(handle1).unwrap();
    assert_eq!(fs.read_from_file(&mut handle2).unwrap().len(), MB)
}

#[test]
fn non_iterable_database_can_be_used_with_fs() {
    struct DummyDatabase;

    impl Database<Vec<u8>, DataContainer<()>> for DummyDatabase {
        fn insert(&mut self, _key: Vec<u8>, _value: DataContainer<()>) -> std::io::Result<()> {
            unimplemented!()
        }

        fn get(&self, _key: &Vec<u8>) -> std::io::Result<DataContainer<()>> {
            unimplemented!()
        }

        fn contains(&self, _key: &Vec<u8>) -> bool {
            unimplemented!()
        }
    }

    let _ = create_cdc_filesystem(DummyDatabase, SimpleHasher);
}

#[test]
fn dedup_ratio_is_correct_for_fixed_size_chunker() {
    let mut fs = create_cdc_filesystem(HashMap::new(), SimpleHasher);

    const MB: usize = 1024 * 1024;
    const CHUNK_SIZE: usize = 4096;

    let data = vec![10; MB];

    // first write => 1 MB, 1 chunk
    let mut fh = fs.create_file("file", FSChunker::new(CHUNK_SIZE)).unwrap();
    fs.write_to_file(&mut fh, &data).unwrap();
    fs.close_file(fh).unwrap();
    assert_relative_eq!(fs.cdc_dedup_ratio(), MB as f64 / CHUNK_SIZE as f64);

    // second write, same data => 2 MBs, 1 chunk
    let mut fh = fs.open_file("file", FSChunker::new(CHUNK_SIZE)).unwrap();
    fs.write_to_file(&mut fh, &data).unwrap();
    fs.close_file(fh).unwrap();
    assert_relative_eq!(fs.cdc_dedup_ratio(), (2 * MB) as f64 / CHUNK_SIZE as f64);

    // third write, different data => 3 MBs, 2 chunks
    let new_data = vec![20; MB];
    let mut fh = fs.open_file("file", FSChunker::new(CHUNK_SIZE)).unwrap();
    fs.write_to_file(&mut fh, &new_data).unwrap();
    fs.close_file(fh).unwrap();

    assert_relative_eq!(
        fs.cdc_dedup_ratio(),
        (3 * MB) as f64 / (CHUNK_SIZE * 2) as f64
    );
}

#[test]
fn readonly_file_handle_cannot_write_can_read() {
    let tempdir = &TempDir::new("storage8").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();
    fs.write_to_file(&mut fh, &[1; MB]).unwrap();
    fs.close_file(fh).unwrap();

    // cannot write
    let mut ro_fh = fs.open_file_readonly("file").unwrap();
    let result = fs.write_to_file(&mut ro_fh, &[1; MB]);
    assert!(result.is_err());
    assert!(result.is_err_and(|e| e.kind() == io::ErrorKind::PermissionDenied));

    // can read complete
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB);
    assert_eq!(read, [1; MB]);

    let read = fs.read_from_file(&mut ro_fh).unwrap();
    assert_eq!(read.len(), MB);
    assert_eq!(read, [1; MB]);

    // can close
    let measurements = fs.close_file(ro_fh).unwrap();
    assert_eq!(measurements, WriteMeasurements::default())
}

#[test]
fn write_from_stream_slice() {
    let tempdir = &TempDir::new("storage9").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();
    fs.write_from_stream(&mut fh, &[1; MB * 2][..]).unwrap();
    fs.close_file(fh).unwrap();

    let ro_fh = fs.open_file_readonly("file").unwrap();
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB * 2);
    assert_eq!(fs.read_file_complete(&ro_fh).unwrap(), vec![1; MB * 2]);
}

#[test]
fn write_from_stream_buf_reader() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[1; MB]).unwrap();
    file.seek(io::SeekFrom::Start(0)).unwrap();

    let tempdir = &TempDir::new("storage10").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path).unwrap(), SimpleHasher);
    let mut fh = fs.create_file("file", FSChunker::default()).unwrap();

    fs.write_from_stream(&mut fh, file).unwrap();
    fs.close_file(fh).unwrap();

    let ro_fh = fs.open_file_readonly("file").unwrap();
    let read = fs.read_file_complete(&ro_fh).unwrap();
    assert_eq!(read.len(), MB);
    assert_eq!(read, [1; MB]);
}
