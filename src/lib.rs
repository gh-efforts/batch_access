use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Chunk {
    pub pos: usize,
    pub data: Vec<u8>
}

pub fn batch_read(
    path: impl AsRef<Path>,
    chunks: &mut [Chunk]
) -> std::io::Result<()> {
    let file = compio::fs::File::open(path)?;
    let file = Arc::new(file);
    let mut jobs = Vec::with_capacity(chunks.len());

    compio::runtime::block_on(async {
        for chunk in &mut *chunks {
            let data = std::mem::take(&mut chunk.data);
            let pos = chunk.pos;
            let file = file.clone();

            let handle = compio::runtime::spawn(async move {
                file.read_at(data, pos).await
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