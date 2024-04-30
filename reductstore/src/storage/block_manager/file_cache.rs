use log::debug;
use reduct_base::error::ReductError;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::sync::RwLock;
use tokio::time::Instant;
use zip::write::FileOptions;

pub(crate) type FileRef = Arc<RwLock<File>>;

const TIME_TO_LIVE: Duration = Duration::from_secs(60);

#[derive(PartialEq)]
enum AccessMode {
    Read,
    ReadWrite,
}

struct FileDescriptor {
    file_ref: FileRef,
    mode: AccessMode,
    used: Instant,
}

#[derive(Clone)]
pub(super) struct FileCache {
    cache: Arc<RwLock<HashMap<PathBuf, FileDescriptor>>>,
    max_size: usize,
}

impl FileCache {
    pub fn new(max_size: usize) -> Self {
        FileCache {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_size,
        }
    }

    pub async fn read(&self, path: &PathBuf) -> Result<FileRef, ReductError> {
        let mut cache = self.cache.write().await;
        let file = if let Some(desc) = cache.get_mut(path) {
            desc.used = Instant::now();
            Arc::clone(&desc.file_ref)
        } else {
            let file = File::options().read(true).open(path).await?;
            let file = Arc::new(RwLock::new(file));
            cache.insert(
                path.clone(),
                FileDescriptor {
                    file_ref: Arc::clone(&file),
                    mode: AccessMode::Read,
                    used: Instant::now(),
                },
            );
            file
        };

        Self::discard_old_descriptors(self.max_size, &mut cache);
        Ok(file)
    }

    pub async fn write_or_create(&self, path: &PathBuf) -> Result<FileRef, ReductError> {
        let mut cache = self.cache.write().await;

        let file = if let Some(desc) = cache.get_mut(path) {
            desc.used = Instant::now();
            if desc.mode == AccessMode::ReadWrite {
                Arc::clone(&desc.file_ref)
            } else {
                // if the file is open in read mode, close it and open in read-write mode
                let rw_file = File::options().write(true).read(true).open(path).await?;
                desc.file_ref = Arc::new(RwLock::new(rw_file));
                desc.mode = AccessMode::ReadWrite;

                Arc::clone(&desc.file_ref)
            }
        } else {
            // if not found, create
            if !tokio::fs::try_exists(path).await? {
                File::create(path).await?;
            };

            let file = File::options().write(true).read(true).open(path).await?;
            let file = Arc::new(RwLock::new(file));
            cache.insert(
                path.clone(),
                FileDescriptor {
                    file_ref: Arc::clone(&file),
                    mode: AccessMode::ReadWrite,
                    used: Instant::now(),
                },
            );
            file
        };

        Self::discard_old_descriptors(self.max_size, &mut cache);
        Ok(file)
    }

    fn discard_old_descriptors(max_size: usize, cache: &mut HashMap<PathBuf, FileDescriptor>) {
        // remove old descriptors
        cache.retain(|_, desc| desc.used.elapsed() < TIME_TO_LIVE);

        // check if the cache is full and remove old
        if cache.len() > max_size {
            let mut oldest: Option<(&PathBuf, &FileDescriptor)> = None;

            for (path, desc) in cache.iter() {
                if let Some(oldest_desc) = oldest {
                    if desc.used < oldest_desc.1.used {
                        oldest = Some((path, desc));
                    }
                } else {
                    oldest = Some((path, desc));
                }
            }

            let path = oldest.unwrap().0.clone();
            cache.remove(&path);
        }
    }
}
