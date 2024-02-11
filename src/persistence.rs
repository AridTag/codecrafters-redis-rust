use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

#[allow(unused)]
#[derive(Debug, Clone)]
pub enum DataType {
    String(String),
    List,
    Set,
    SortedSet,
    Hash,
    ZipMap,
    ZipList,
    IntSet,
    SortedSetZipList,
    HashMapZipList,
    ListQuickList,
}

pub struct RdbData {
    pub rdb_version: u16,
    pub metadata: HashMap<String, String>,
    pub databases: HashMap<usize, HashMap<String, DataType>>,
}

#[derive(Error, Debug)]
pub enum RdbReadError {
    #[error("File is not a redis database")]
    NotRedisDatabase,

    #[error("IO Error")]
    IoError(#[from] tokio::io::Error),

    #[error("Error reading utf8 string")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("Failed to parse int")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Invalid length encoding bits: {0:8b}")]
    InvalidLengthEncoding(u8),

    #[error("SpecialFormat LengthEncoding is invalid for length encoded integer")]
    SpecialFormatInvalidIntEncoded,

    #[error("Invalid flag when reading Expiry Timestamp {0:02X}")]
    InvalidExpiryTimestampFlag(u8),

    #[error("Attempted to read key without a database selected")]
    AttemptReadKeyWithoutDatabaseSelected,
}

pub struct RdbReader;

impl RdbReader {
    pub async fn read(path: impl AsRef<Path>) -> Result<RdbData, RdbReadError> {
        let mut reader = {
            let file = File::open(path).await?;
            BufReader::new(file)
        };

        if !Self::is_rdb_file(&mut reader).await? {
            return Err(RdbReadError::NotRedisDatabase);
        }

        let rdb_version = {
            let mut buff = [0u8; 4];
            reader.read_exact(&mut buff).await?;
            let ver_str = std::str::from_utf8(&buff)?;
            u16::from_str(ver_str)?
        };

        let mut metadata = HashMap::new();
        let mut databases: HashMap<usize, HashMap<String, DataType>> = HashMap::new();
        let mut current_database: Option<usize> = None;
        loop {
            let opcode = reader.read_u8().await?;
            match opcode {
                0xFA => {
                    // Auxiliary data
                    let key = reader.read_string_encoded().await?;
                    let value = reader.read_string_encoded().await?;
                    metadata.insert(key, value);
                }
                0xFB => {
                    let db_table_size = reader.read_length_encoded_int().await?;
                    let expiry_table_size = reader.read_length_encoded_int().await?;
                    println!("Table sizes: {} -- {}", db_table_size, expiry_table_size);
                }
                0xFC | 0xFD => {
                    if current_database.is_none() {
                        return Err(RdbReadError::AttemptReadKeyWithoutDatabaseSelected);
                    }

                    // TODO: Handle expiry
                    let _expire_timestamp = reader.read_expiry_timestamp().await?;
                    let (key, value) = reader.read_key_value(None).await?;
                    let database = databases.entry(current_database.unwrap()).or_insert_with(|| HashMap::new());
                    database.insert(key, value);
                }
                0xFE => {
                    let database = reader.read_u8().await?;
                    current_database = Some(database.into());
                }
                0xFF => {
                    // End of rdb.
                    let _crc64 = reader.read_u64().await?;
                    break;
                }
                _ => {
                    if current_database.is_none() {
                        return Err(RdbReadError::AttemptReadKeyWithoutDatabaseSelected);
                    }

                    let (key, value) = reader.read_key_value(Some(opcode)).await?;
                    let database = databases.entry(current_database.unwrap()).or_insert_with(|| HashMap::new());
                    database.insert(key, value);
                }
            }


        }

        // TODO: Discard expired keys

        Ok(RdbData {
            rdb_version,
            metadata,
            databases,
        })
    }

    async fn is_rdb_file(reader: &mut BufReader<File>) -> Result<bool, RdbReadError> {
        reader.seek(SeekFrom::Start(0)).await?;

        let mut buff = [0u8; 5];
        reader.read_exact(&mut buff).await?;
        Ok(buff.cmp(b"REDIS") == Ordering::Equal)
    }
}

trait RdbBufReader {
    async fn read_length_encoded_int(&mut self) -> Result<usize, RdbReadError>;
    async fn read_string_encoded(&mut self) -> Result<String, RdbReadError>;
    async fn read_expiry_timestamp(&mut self) -> Result<ExpiryTimestamp, RdbReadError>;
    async fn read_key_value(&mut self, known_type: Option<u8>) -> Result<(String, DataType), RdbReadError>;

    async fn read_length_encoding(reader: &mut BufReader<File>) -> Result<(LengthEncoding, usize), RdbReadError> {
        let length = reader.read_u8().await?;
        let (encoding, length) = {
            let mask = 0b11000000u8;
            let remaining_bits = length & !mask;
            match (length & mask) >> 6 {
                0b00 => (LengthEncoding::Remaining6Bits, remaining_bits),
                0b01 => (LengthEncoding::RemainingAndNextByte, remaining_bits),
                0b10 => (LengthEncoding::DiscardRemainingGetNext4Bytes, 0),
                0b11 => (LengthEncoding::SpecialFormat, remaining_bits),
                x => return Err(RdbReadError::InvalidLengthEncoding(x))
            }
        };
        Ok((encoding, length as usize))
    }

    async fn interpret_length_encoding(reader: &mut BufReader<File>, length_encoding: LengthEncoding, length: usize) -> Result<usize, RdbReadError> {
        let value = match length_encoding {
            LengthEncoding::Remaining6Bits => length,
            LengthEncoding::DiscardRemainingGetNext4Bytes => reader.read_u32_le().await? as usize,
            LengthEncoding::RemainingAndNextByte => (length << 8) | (reader.read_u8().await? as usize),
            LengthEncoding::SpecialFormat => return Err(RdbReadError::SpecialFormatInvalidIntEncoded),
        };

        Ok(value)
    }

    async fn read_value_type(reader: &mut BufReader<File>, value_type: u8) -> Result<DataType, RdbReadError> {
        let value = match value_type {
            0 => DataType::String(reader.read_string_encoded().await?),
            _ => todo!("DataType isn't handled yet!")
        };

        Ok(value)
    }
}

impl RdbBufReader for BufReader<File> {
    async fn read_length_encoded_int(&mut self) -> Result<usize, RdbReadError> {
        let (encoding, length) = Self::read_length_encoding(self).await?;
        let value = Self::interpret_length_encoding(self, encoding, length).await?;

        Ok(value)
    }

    async fn read_string_encoded(&mut self) -> Result<String, RdbReadError> {
        let (encoding, length) = Self::read_length_encoding(self).await?;
        if encoding == LengthEncoding::SpecialFormat {
            let value = match length {
                0 => self.read_u8().await? as usize,
                1 => self.read_u16_le().await? as usize,
                2 => self.read_u32_le().await? as usize,
                3 => todo!("Compressed string not implemented"),
                _ => panic!("Invalid SpecialFormat for string encoding! {}", length)
            };

            Ok(value.to_string())
        } else {
            let length = Self::interpret_length_encoding(self, encoding, length).await?;
            let mut buff = Vec::new();
            buff.resize(length, 0u8);
            self.read_exact(&mut buff).await?;

            Ok(String::from_utf8_lossy(&buff).to_string())
        }
    }

    async fn read_expiry_timestamp(&mut self) -> Result<ExpiryTimestamp, RdbReadError> {
        let flag = self.read_u8().await?;
        let value = match flag {
            0xFD => ExpiryTimestamp::Seconds(self.read_u32_le().await?),
            0xFC => ExpiryTimestamp::Milliseconds(self.read_u64_le().await?),
            _ => return Err(RdbReadError::InvalidExpiryTimestampFlag(flag)),
        };
        Ok(value)
    }

    async fn read_key_value(&mut self, known_type: Option<u8>) -> Result<(String, DataType), RdbReadError> {
        let value_type = {
            if known_type.is_some() {
                known_type.unwrap()
            } else {
                self.read_u8().await?
            }
        };

        let key = self.read_string_encoded().await?;
        let value = Self::read_value_type(self, value_type).await?;
        Ok((key, value))
    }
}

#[allow(unused)]
enum ExpiryTimestamp {
    Seconds(u32),
    Milliseconds(u64),
}

#[derive(Debug, Eq, PartialEq)]
enum LengthEncoding {
    Remaining6Bits,
    RemainingAndNextByte,
    DiscardRemainingGetNext4Bytes,
    SpecialFormat,
}