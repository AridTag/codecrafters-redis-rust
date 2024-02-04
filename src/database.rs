use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

static CACHE: Lazy<Arc<RwLock<HashMap<String, CacheEntry>>>> = Lazy::new(|| {
    Arc::new(RwLock::new(HashMap::new()))
});

struct CacheEntry {
    creation_time: u128,
    timeout: Option<Duration>,
    value: String,
}

#[derive(Debug)]
pub enum RespDataType {
    //Error(String),
    //SimpleString(String),
    //Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespDataType>),
    //NullArray,
    //NullBulkString,
}

pub async fn db_get(key: &String) -> Result<Option<String>, anyhow::Error> {
    let (result, should_remove) = {
        let cache = CACHE.read().await;
        let mut is_valid = true;
        if let Some(entry) = cache.get(key) {
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
    };

    if should_remove {
        let mut cache = CACHE.write().await;
        cache.remove(key);
    }

    Ok(result)
}

pub async fn db_set(key: String, value: String, timeout: Option<Duration>) -> Result<(), anyhow::Error> {
    let mut cache = CACHE.write().await;

    let entry = CacheEntry {
        creation_time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis(),
        value,
        timeout
    };
    cache.insert(key, entry);

    Ok(())
}