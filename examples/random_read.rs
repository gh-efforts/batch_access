use std::path::PathBuf;
use rand::Rng;
use batch_access::{batch_read, Chunk};

pub fn main() {
    let mut args = std::env::args();
    args.next();
    let file_path = args.next().expect("need file path");
    let file_path = PathBuf::from(file_path);
    let metadata = file_path.metadata().expect("file not exists");
    let file_len = metadata.len();

    let mut chunks = vec![Chunk { data: Vec::with_capacity(32), pos: 0 }; 26 * 10000];
    let mut rng = rand::thread_rng();

    for x in &mut chunks {
        x.pos = rng.gen_range(0..(file_len as usize - 31));
    }

    batch_read(file_path, &mut chunks).unwrap();
}