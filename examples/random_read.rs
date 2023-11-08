use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use rand::Rng;
use batch_access::{Chunk, par_batch_read};

pub fn main() {
    let mut args = std::env::args();
    args.next();
    let file_path = args.next().expect("need file path");
    let chunks_num = args.next().expect("need chunks number");
    let threads = args.next()
        .map(|s| usize::from_str(&s).expect("invalid threads number"))
        .unwrap_or(std::thread::available_parallelism().unwrap().get());

    let num = usize::from_str(&chunks_num).expect("invalid chunks number");
    let file_path = PathBuf::from(file_path);
    let metadata = file_path.metadata().expect("file does not exist");
    let file_len = metadata.len();

    println!("alloc temp memory");

    let mut chunks = vec![Chunk { data: Vec::with_capacity(32), pos: 0 }; num];
    let mut rng = rand::thread_rng();

    println!("gen random pos");
    for x in &mut chunks {
        x.pos = rng.gen_range(0..(file_len as usize - 31));
    }

    println!("start batch read");
    let t = Instant::now();
    par_batch_read(file_path, &mut chunks, threads).unwrap();
    println!("end batch read, use {:?}", t.elapsed())
}