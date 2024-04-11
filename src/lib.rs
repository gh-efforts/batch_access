use std::path::Path;
use std::rc::Rc;

use monoio::RuntimeBuilder;
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSliceMut;

#[cfg(target_os = "linux")]
type MonoioDriver = monoio::IoUringDriver;

#[cfg(not(target_os = "linux"))]
type MonoioDriver = monoio::LegacyDriver;

#[derive(Clone)]
pub struct Chunk {
    pub pos: usize,
    pub data: Vec<u8>,
}

pub fn batch_read(
    path: impl AsRef<Path>,
    chunks: &mut [&mut Chunk],
) -> std::io::Result<()> {
    let mut jobs = Vec::with_capacity(chunks.len());

    let builder: RuntimeBuilder<MonoioDriver> = monoio::RuntimeBuilder::new();
    let mut rt = builder
        .build()?;

    rt.block_on(async {
        let file = monoio::fs::File::open(path).await?;
        let file = Rc::new(file);

        for chunk in &mut *chunks {
            let data = std::mem::take(&mut chunk.data);
            let pos = chunk.pos;
            let file = file.clone();

            let handle = monoio::spawn(async move {
                file.read_exact_at(data, pos as u64).await
            });
            jobs.push(handle);
        }

        let mut ret = Ok(());

        for (job, chunk) in jobs.into_iter().zip(chunks) {
            let res = job.await;
            chunk.data = res.1;

            if res.0.is_err() {
                ret = res.0;
            }
        }

        let file = Rc::into_inner(file).unwrap();
        file.close().await?;
        ret
    })
}

pub fn current_par_batch_read2(
    path: impl AsRef<Path> + Sync,
    chunks: &mut [&mut Chunk],
    batch_len: usize,
) -> std::io::Result<()> {
    let res = chunks
        .par_chunks_mut(batch_len)
        .map(|x| batch_read(&path, x))
        .collect::<std::io::Result<Vec<_>>>();

    res.map(|_| ())
}

pub fn current_par_batch_read(
    path: impl AsRef<Path> + Sync,
    chunks: &mut [Chunk],
    batch_len: usize,
) -> std::io::Result<()> {
    let mut chunks = chunks.iter_mut()
        .map(|v| v)
        .collect::<Vec<_>>();

    current_par_batch_read2(path, &mut chunks, batch_len)
}

pub fn par_batch_read(
    path: impl AsRef<Path> + Send + Sync,
    chunks: &mut [Chunk],
    threads: usize,
) -> std::io::Result<()> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    pool.install(|| {
        let batch_len = chunks.len() / threads;
        current_par_batch_read(path, chunks, batch_len)
    })
}