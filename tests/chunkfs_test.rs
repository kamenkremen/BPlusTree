extern crate chunkfs;

use std::path::PathBuf;

use bplus_tree::bplus_tree::BPlus;
use chunkfs::chunkers::{FSChunker, LeapChunker};
use chunkfs::create_cdc_filesystem;
use chunkfs::hashers::SimpleHasher;
use tempdir::TempDir;

const MB: usize = 1024 * 1024;

#[test]
fn write_read_complete_test() {
    let tempdir = &TempDir::new("storage1").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut fs = create_cdc_filesystem(BPlus::new(100, path), SimpleHasher);

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
    let mut fs = create_cdc_filesystem(BPlus::new(100, path), SimpleHasher);
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
    let mut fs = create_cdc_filesystem(BPlus::new(100, path), SimpleHasher);

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
    let mut fs = create_cdc_filesystem(BPlus::new(100, path), SimpleHasher);

    let mut handle = fs.create_file("file", FSChunker::new(4096)).unwrap();

    let data = vec![1; 64 * MB + 50];
    fs.write_to_file(&mut handle, &data).unwrap();
    fs.close_file(handle).unwrap();

    let handle = fs.open_file("file", LeapChunker::default()).unwrap();
    assert_eq!(fs.read_file_complete(&handle).unwrap().len(), data.len());
}
