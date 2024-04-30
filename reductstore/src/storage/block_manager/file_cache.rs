use log::debug;
use reduct_base::error::ReductError;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::RwLock;
use tokio::time::Instant;
use zip::write::FileOptions;

pub(crate) type FileRef = Arc<RwLock<File>>;

pub(super) struct FileCache {
    cache: HashMap<PathBuf, (FileRef, Instant)>,
    max_size: usize,
}

impl FileCache {
    pub fn new(max_size: usize) -> Self {
        FileCache {
            cache: HashMap::new(),
            max_size,
        }
    }

    pub async fn get_or_create(&mut self, path: &PathBuf) -> Result<FileRef, ReductError> {
        let file = if let Some((file, last_update)) = self.cache.get_mut(path) {
            *last_update = Instant::now();
            Arc::clone(file)
        } else {
            // if not found, create
            if !tokio::fs::try_exists(path).await? {
                File::create(path).await?;
            };

            let file = File::options().write(true).read(true).open(path).await?;

            let file = Arc::new(RwLock::new(file));
            self.cache
                .insert(path.clone(), (Arc::clone(&file), Instant::now()));
            file
        };

        // check if the cache is full and remove old
        if self.cache.len() > self.max_size {
            let mut oldest = None;

            for (path, (_, last_update)) in self.cache.iter() {
                if let Some((_, oldest_last_update)) = oldest {
                    if last_update < &oldest_last_update {
                        oldest = Some((path, *last_update));
                    }
                } else {
                    oldest = Some((path, *last_update));
                }
            }

            let path = oldest.unwrap().0.clone();
            self.cache.remove(&path);
            debug!("Removed file from cache: {:?}", path.clone());
        }

        Ok(file)
    }
}
