use std::path::Path;
use std::sync::Arc;

use monoio::{IoUringDriver, RuntimeBuilder};
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSliceMut;

#[derive(Clone)]
pub struct Chunk {
    pub pos: usize,
    pub data: Vec<u8>,
}

pub fn batch_read(
    path: impl AsRef<Path>,
    chunks: &mut [Chunk],
) -> std::io::Result<()> {
    let mut jobs = Vec::with_capacity(chunks.len());

    let builder: RuntimeBuilder<IoUringDriver> = monoio::RuntimeBuilder::new();
    let mut rt = builder
        .build()?;

    rt.block_on(async {
        let file = monoio::fs::File::open(path).await?;
        let file = Arc::new(file);

        for chunk in &mut *chunks {
            let data = std::mem::take(&mut chunk.data);
            let pos = chunk.pos;
            let file = file.clone();

            let handle = monoio::spawn(async move {
                file.read_exact_at(data, pos as u64).await
            });
            jobs.push(handle);
        }

        for (job, chunk) in jobs.into_iter().zip(chunks) {
            let res = job.await;
            chunk.data = res.1;
            res.0?;
        }

        Ok(())
    })
}

pub fn par_batch_read(
    path: impl AsRef<Path> + Sync,
    chunks: &mut [Chunk],
    threads: usize,
) -> std::io::Result<()> {
    let batch_len = chunks.len() / threads;

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    pool.install(|| {
        let res = chunks
            .par_chunks_mut(batch_len)
            .map(|x| batch_read(&path, x))
            .collect::<std::io::Result<Vec<_>>>();

        res.map(|_| ())
    })
}