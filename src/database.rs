use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use crate::persistence::{DataType, RdbReader};

type Database = HashMap<String, CacheEntry>;

static CACHE: Lazy<Arc<RwLock<HashMap<usize, Database>>>> = Lazy::new(|| {
    let mut databases = HashMap::new();
    for i in 0..16 {
        databases.insert(i, Database::new());
    }
    Arc::new(RwLock::new(databases))
});

struct CacheEntry {
    creation_time: u128,
    timeout: Option<Duration>,
    value: DataType,
}

pub async fn db_load(db_file: impl AsRef<Path>) -> Result<(), anyhow::Error> {
    let mut cache = CACHE.write().await;
    cache.clear();

    let data = match RdbReader::read(db_file).await {
        Ok(r) => r,
        Err(e) => {
            println!("Failed to open database - {:?}", e);
            return Ok(());
        }
    };

    for (id, map) in data.databases {
        let remapped = map
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    CacheEntry {
                        creation_time: 0,
                        timeout: None,
                        value: v,
                    }
                )
            })
            .collect();
        cache.insert(id, remapped);
    }

    Ok(())
}

pub async fn _db_save(_file_path: &Path) {}

pub async fn db_get(db_id: usize, key: &String) -> Result<Option<DataType>, anyhow::Error> {
    let (result, should_remove) = {
        let cache = CACHE.read().await;
        if let Some(database) = cache.get(&db_id) {
            let mut is_valid = true;
            if let Some(entry) = database.get(key) {
                let current_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                if let Some(timeout) = entry.timeout {
                    if (current_time - entry.creation_time) >= timeout.as_millis() {
                        is_valid = false;
                    }
                }

                if is_valid {
                    (Some(entry.value.clone()), false)
                } else {
                    (None, true)
                }
            } else {
                (None, false)
            }
        } else {
            (None, false)
        }
    };

    if should_remove {
        let mut cache = CACHE.write().await;
        let database = cache.get_mut(&db_id).unwrap();
        database.remove(key);
    }

    Ok(result)
}

pub async fn db_set(db_id: usize, key: String, value: String, timeout: Option<Duration>) -> Result<(), anyhow::Error> {
    let mut cache = CACHE.write().await;
    if let Some(database) = cache.get_mut(&db_id) {
        let entry = CacheEntry {
            creation_time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis(),
            value: DataType::String(value),
            timeout,
        };
        database.insert(key, entry);
    }

    Ok(())
}

pub async fn db_list_keys(db_id: usize) -> Result<Vec<String>, anyhow::Error> {
    let cache = CACHE.read().await;
    if let Some(database) = cache.get(&db_id) {
        Ok(database.keys().cloned().collect::<Vec<_>>())
    } else {
        Err(anyhow::Error::msg("Database doesn't exist"))
    }
}
