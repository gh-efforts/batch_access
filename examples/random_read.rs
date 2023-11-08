use std::io::Write;
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

    let mut chunks = Vec::with_capacity(num);
    let mut rng = rand::thread_rng();

    println!("gen random pos");

    for _ in 0..num {
        let chunk = Chunk { data: Vec::with_capacity(32), pos: rng.gen_range(0..(file_len as usize - 31)) };
        chunks.push(chunk);
    }

    chunks.sort_unstable_by_key(|c| c.pos);
    println!("start batch read");
    let t = Instant::now();
    par_batch_read(file_path, &mut chunks, threads).unwrap();
    println!("end batch read, use {:?}", t.elapsed());

    let file = std::fs::File::options()
        .create(true)
        .write(true)
        .open("tempfile")
        .expect("failed to open file");

    let mut file = std::io::BufWriter::new( file);

    for chunk in chunks {
        file.write_all(&chunk.data).unwrap();
    }

    file.flush().unwrap();
}