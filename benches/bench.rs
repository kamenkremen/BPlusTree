use std::fs;
use std::fs::File;
use std::path::PathBuf;

use bplus_tree::bplus_tree::BPlus;
use criterion::measurement::WallTime;
use criterion::{criterion_group, BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};

use chunkfs::chunkers::UltraChunker;
use chunkfs::create_cdc_filesystem;
use chunkfs::hashers::Sha256Hasher;
use tempdir::TempDir;

struct Dataset<'a> {
    path: &'a str,
    name: &'a str,
    size: u64,
}

impl<'a> Dataset<'a> {
    fn new(path: &'a str, name: &'a str) -> Self {
        let size = File::open(path).unwrap().metadata().unwrap().len();
        Dataset { path, name, size }
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let datasets = vec![
        //Dataset::new("linux.tar", "linux"),
        //Dataset::new("ubuntu.iso", "ubuntu"),
        Dataset::new("datasets/dummy-file.txt", "dummy"),
    ];

    for dataset in datasets {
        let mut group = c.benchmark_group("Chunkers");
        group.sample_size(60);
        group.throughput(Throughput::Bytes(dataset.size));

        let data = fs::read(dataset.path).unwrap();

        bench_write(&dataset, &mut group, &data);

        bench_read(&dataset, &mut group, &data);
    }
}

fn bench_write(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    group.bench_function(BenchmarkId::new("write", dataset.name), |b| {
        b.iter_batched(
            || {
                let tempdir =
                    TempDir::new(format!("{}", rand::random::<usize>()).as_str()).unwrap();
                let base = BPlus::new(200, PathBuf::new().join(tempdir.path())).unwrap();
                let mut fs = create_cdc_filesystem(base, Sha256Hasher::default());

                let chunker = UltraChunker::default();
                let handle = fs.create_file("file", chunker).unwrap();

                (fs, handle)
            },
            |(mut fs, mut handle)| {
                fs.write_to_file(&mut handle, data).unwrap();
                fs.close_file(handle).unwrap();
            },
            BatchSize::PerIteration,
        )
    });
}

fn bench_read(dataset: &Dataset, group: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    group.bench_function(BenchmarkId::new("read-ultra", dataset.name), |b| {
        b.iter_batched(
            || {
                let tempdir =
                    TempDir::new(format!("{}", rand::random::<usize>()).as_str()).unwrap();
                let base = BPlus::new(200, PathBuf::new().join(tempdir.path())).unwrap();
                let mut fs = create_cdc_filesystem(base, Sha256Hasher::default());

                let chunker = UltraChunker::default();
                let mut handle = fs.create_file("file", chunker).unwrap();
                fs.write_to_file(&mut handle, data).unwrap();
                fs.close_file(handle).unwrap();

                let chunker = UltraChunker::default();
                let handle = fs.open_file("file", chunker).unwrap();

                (fs, handle)
            },
            |(fs, handle)| {
                fs.read_file_complete(&handle).unwrap();
            },
            BatchSize::PerIteration,
        )
    });
}

criterion_group!(benches, criterion_benchmark);

fn main() {
    benches();

    Criterion::default().configure_from_args().final_summary();
}
