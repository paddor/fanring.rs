//! Append-only JSONL result files, one per implementation directory.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

pub(crate) struct JsonlResults {
    root: PathBuf,
    file_name: &'static str,
    writers: BTreeMap<String, BufWriter<File>>,
}

impl JsonlResults {
    pub(crate) fn new(file_name: &'static str) -> Self {
        Self {
            root: results_root(),
            file_name,
            writers: BTreeMap::new(),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn write<T: Serialize>(&mut self, implementation: &str, row: &T) {
        let directory = implementation_directory(implementation);
        let path = result_path(&self.root, implementation, self.file_name);
        let writer = self
            .writers
            .entry(directory.to_string())
            .or_insert_with(|| {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).expect("create benchmark result directory");
                }
                append_jsonl(&path)
            });
        serde_json::to_writer(&mut *writer, row).expect("write benchmark row");
        writeln!(writer).expect("write benchmark newline");
    }

    pub(crate) fn flush(&mut self) {
        for writer in self.writers.values_mut() {
            writer.flush().expect("flush benchmark JSONL");
        }
    }
}

fn implementation_directory(implementation: &str) -> &str {
    implementation
        .strip_suffix("-mpmc")
        .unwrap_or(implementation)
}

pub(super) fn result_path(root: &Path, implementation: &str, file_name: &str) -> PathBuf {
    root.join(implementation_directory(implementation))
        .join(file_name)
}

fn results_root() -> PathBuf {
    std::env::var_os("FANRING_BENCH_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(
                std::env::var_os("HOME")
                    .expect("HOME must be set unless FANRING_BENCH_CACHE_DIR is set"),
            )
            .join(".cache/fanring")
        })
}

fn append_jsonl(path: &Path) -> BufWriter<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .expect("open benchmark JSONL");
    let length = file.metadata().expect("read benchmark metadata").len();
    if length != 0 {
        file.seek(SeekFrom::End(-1))
            .expect("seek benchmark JSONL tail");
        let mut last = [0];
        file.read_exact(&mut last)
            .expect("read benchmark JSONL tail");
        if last[0] != b'\n' {
            file.write_all(b"\n")
                .expect("terminate partial benchmark row");
        }
    }
    BufWriter::new(file)
}
