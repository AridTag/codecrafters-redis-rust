use std::fmt::{Display, Formatter};
use std::time::Duration;
use std::io::Write;
use std::str::FromStr;
use bytes::buf::Writer;
use bytes::BufMut;
use futures::future::BoxFuture;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::CONFIG;
use crate::database::{db_get, db_list_keys, db_set};
use crate::persistence::DataType;

#[derive(Debug)]
pub enum ResponseType {
    //Error(String),
    //SimpleString(String),
    //Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<ResponseType>),
    //NullArray,
    //NullBulkString,
}

impl Display for ResponseType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResponseType::{:?}", self)
    }
}

impl ResponseType {
    pub fn string(&self) -> Option<String> {
        match self {
            ResponseType::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
            _ => None,
        }
    }
}

#[derive(Error, Debug)]
pub enum RespProtocolError {
    #[error("Received too many bytes before reaching end of message")]
    MessageTooBig,

    #[error("Unhandled data type: {0}")]
    UnhandledRespDataType(char),

    #[error("Array number of elements specifier is not a valid integer: '{0}'")]
    ArrayNumElementsInvalidLength(String),

    #[error("BulkString length specifier is not a valid integer: '{0}'")]
    BulkStringInvalidLength(String),
}

pub struct RedisClientConnection {
    stream: TcpStream,
    read_buffer: [u8; 512],
    write_index: usize,
    selected_db: usize,
}

impl RedisClientConnection {
    pub const fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buffer: [0u8; 512],
            write_index: 0,
            selected_db: 0,
        }
    }

    pub async fn process(&mut self) -> Result<(), anyhow::Error> {
        loop {
            let request = self.read().await?;
            match request {
                ResponseType::Array(elements) => {
                    if !elements.is_empty() {
                        if let ResponseType::BulkString(command) = &elements[0] {
                            let command = String::from_utf8_lossy(command).to_string();
                            handle_command(self, command, &elements[1..]).await?;
                        }
                    }
                }

                _ => todo!("Unhandled datatype in request")
            }
        }
    }

    pub async fn read(&mut self) -> Result<ResponseType, anyhow::Error> {
        fn slide_window(buffer: &mut [u8; 512], start: usize, length: usize) {
            let (from, to) = buffer.split_at_mut(start);
            to[..length].copy_from_slice(&from[..length]);
        }

        loop {
            if self.write_index >= self.read_buffer.len() {
                return Err(RespProtocolError::MessageTooBig.into());
            }

            let bytes_read = self.stream.read(&mut self.read_buffer[self.write_index..]).await?;
            if bytes_read == 0 {
                continue;
            }

            self.write_index += bytes_read;
            let end_index = self.write_index;
            let request = Self::parse_resp(&self.read_buffer[0..end_index])?;
            if let Some(RespParseResult { request, consumed }) = request {
                slide_window(&mut self.read_buffer, consumed, self.write_index);
                self.write_index -= consumed;

                return Ok(request);
            }
        }
    }


    fn parse_resp(buffer: &[u8]) -> Result<Option<RespParseResult>, RespProtocolError> {
        let part_end = Self::get_next_part_end(buffer);
        if part_end.is_none() {
            return Ok(None);
        }

        let part_end = part_end.unwrap();
        if part_end == 0 {
            return Ok(None);
        }

        let prefix_end = part_end - 1;
        let request = match buffer[0] {
            b'*' => Self::parse_array(&buffer[1..prefix_end], &buffer[part_end + 1..])?,
            b'$' => Self::parse_bulk_string(&buffer[1..prefix_end], &buffer[part_end + 1..])?,
            x => return Err(RespProtocolError::UnhandledRespDataType(x as char))
        };

        if request.is_none() {
            return Ok(None);
        }

        let RespParseResult { request, consumed } = request.unwrap();
        let consumed = consumed + (prefix_end + 2);

        Ok(Some(
            RespParseResult {
                request,
                consumed,
            }
        ))
    }

    fn get_next_part_end(buffer: &[u8]) -> Option<usize> {
        for i in 0..buffer.len() {
            if buffer[i] == b'\r' && (i + 1) < buffer.len() && buffer[i + 1] == b'\n' {
                return Some(i + 1);
            }
        }

        None
    }

    fn parse_bulk_string(string_part: &[u8], remainder: &[u8]) -> Result<Option<RespParseResult>, RespProtocolError> {
        let length = String::from_utf8_lossy(string_part);
        let Ok(length) = length.parse::<i32>() else {
            return Err(RespProtocolError::BulkStringInvalidLength(length.to_string()));
        };

        if length < 0 {
            return Err(RespProtocolError::BulkStringInvalidLength(length.to_string()));
        }

        let length = length as usize;
        Ok(Some(
            RespParseResult {
                request: ResponseType::BulkString(remainder[..length].to_vec()),
                consumed: length,
            }
        ))
    }

    fn parse_array(array_part: &[u8], mut remainder: &[u8]) -> Result<Option<RespParseResult>, RespProtocolError> {
        let num_elements = String::from_utf8_lossy(array_part);
        let Ok(num_elements) = num_elements.parse::<i32>() else {
            return Err(RespProtocolError::ArrayNumElementsInvalidLength(num_elements.to_string()));
        };

        let mut consumed = 0;
        let mut elements = vec![];
        for _ in 0..num_elements {
            let next_part_end = Self::get_next_part_end(remainder);
            let Some(_) = next_part_end else {
                return Ok(None);
            };

            let result = Self::parse_resp(remainder)?;
            let Some(element) = result else {
                return Ok(None);
            };

            consumed += element.consumed + 2;
            elements.push(element.request);

            remainder = &remainder[element.consumed + 2..];
        }

        Ok(Some(
            RespParseResult {
                consumed,
                request: ResponseType::Array(elements),
            }
        ))
    }
}

struct RespParseResult {
    request: ResponseType,
    consumed: usize,
}

fn write_ok(buffer: &mut Writer<Vec<u8>>) -> tokio::io::Result<()> {
    write_simple_string(buffer, b"OK")?;
    Ok(())
}

fn write_simple_string(buffer: &mut Writer<Vec<u8>>, string: &[u8]) -> tokio::io::Result<()> {
    buffer.write_all(format!("+{}\r\n", String::from_utf8_lossy(string)).as_bytes())?;
    Ok(())
}

fn write_simple_error(buffer: &mut Writer<Vec<u8>>, string: &[u8]) -> tokio::io::Result<()> {
    buffer.write_all(format!("-{}\r\n", String::from_utf8_lossy(string)).as_bytes())?;
    Ok(())
}

fn write_nil_bulk_string(buffer: &mut Writer<Vec<u8>>) -> tokio::io::Result<()> {
    buffer.write_all(b"$-1\r\n")?;
    Ok(())
}

fn write_bulk_string(buffer: &mut Writer<Vec<u8>>, string: &[u8]) -> tokio::io::Result<()> {
    let length_str = string.len().to_string();
    buffer.write_all(format!("${}\r\n{}\r\n", length_str, String::from_utf8_lossy(string)).as_bytes())?;
    Ok(())
}

enum Command {
    Echo,
    Ping,
    #[allow(clippy::enum_variant_names)]
    Command,
    Select,
    Set,
    Get,
    Config,
    Keys,
    Info
}

impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match s.to_lowercase().as_str() {
            "echo" => Command::Echo,
            "ping" => Command::Ping,
            "command" => Command::Command,
            "select" => Command::Select,
            "set" => Command::Set,
            "get" => Command::Get,
            "config" => Command::Config,
            "keys" => Command::Keys,
            "info" => Command::Info,
            _ => anyhow::bail!("Invalid Command {}", s)
        };

        Ok(res)
    }
}

async fn handle_command(client: &mut RedisClientConnection, command: String, arguments: &[ResponseType]) -> Result<(), anyhow::Error> {
    let mut response_buff = Vec::with_capacity(256).writer();
    match Command::from_str(command.as_str())? {
        Command::Echo => {
            if let Some(string) = arguments[0].string() {
                write_bulk_string(&mut response_buff, string.as_bytes())?;
            }
        }

        Command::Ping => {
            write_simple_string(&mut response_buff, b"PONG")?;
        }

        Command::Command => {
            // TODO: Need to implement this I guess
            write_simple_error(&mut response_buff, b"COMMAND not implemented")?;
        }

        Command::Select => {
            if !arguments.is_empty() {
                if let Some(id_string) = arguments[0].string() {
                    let id = id_string.parse::<usize>()?;
                    client.selected_db = id;
                    write_ok(&mut response_buff)?;
                    println!("Client selected db {}", client.selected_db);
                }
            }
        }

        Command::Set => {
            let mut success = false;
            if arguments.len() >= 2 {
                let mut timeout = None;
                if arguments.len() >= 4 {
                    if let Some(option) = arguments[2].string() {
                        if let Some(value) = arguments[3].string() {
                            match option.as_str() {
                                "PX" | "px" => {
                                    timeout = Some(Duration::from_millis(value.parse::<u64>()?));
                                }

                                "EX" | "ex" => {
                                    timeout = Some(Duration::from_secs(value.parse::<u64>()?));
                                }

                                _ => { }
                            }
                        }
                    }
                }

                if let Some(key) = arguments[0].string() {
                    if let Some(value) = arguments[1].string() {
                        db_set(client.selected_db, key, value, timeout).await?;
                        write_ok(&mut response_buff)?;
                        success = true;
                    }
                }
            }

            if !success {
                write_simple_error(&mut response_buff, b"Failed to set")?;
            }
        }

        Command::Get => {
            let mut success = false;
            if !arguments.is_empty() {
                if let Some(key) = arguments[0].string() {
                    if let Ok(Some(DataType::String(value))) = db_get(client.selected_db, &key).await {
                        write_bulk_string(&mut response_buff, value.as_bytes())?;
                        success = true;
                    }
                }
            }

            if !success {
                write_nil_bulk_string(&mut response_buff)?;
            }
        }

        Command::Config => {
            if !arguments.is_empty() {
                if let Some(command) = arguments[0].string() {
                    match command.as_str() {
                        "GET" | "get" => {
                            let mut responses: Vec<(&str, Option<String>)> = vec![];
                            for data_arg in &arguments[1..] {
                                if let Some(option) = data_arg.string() {
                                    match option.as_str() {
                                        "DIR" | "dir" => {
                                            let value = CONFIG.read().await.dir.clone();
                                            responses.push(("dir", value));
                                        }

                                        "DBFILENAME" | "dbfilename" => {
                                            let value = CONFIG.read().await.dir.clone();
                                            responses.push(("dbfilename", value));
                                        }

                                        _ => { }
                                    }
                                }
                            }

                            response_buff.write_all(format!("*{}\r\n", responses.len() * 2).as_bytes())?;
                            for (key, value) in &responses {
                                write_bulk_string(&mut response_buff, key.as_bytes())?;
                                if let Some(value) = value {
                                    write_bulk_string(&mut response_buff, value.as_bytes())?;
                                } else {
                                    write_nil_bulk_string(&mut response_buff)?;
                                }
                            }
                        }

                        "SET" | "set" => {

                        }

                        "REWRITE" | "rewrite" => {

                        }

                        "RESETSTAT" | "resetstat" => {

                        }

                        _ => { }
                    }
                }
            }
        }

        Command::Keys => {
            if !arguments.is_empty() {
                if let Some(arg) = arguments[0].string() {
                    if arg == "*" {
                        let keys = db_list_keys(client.selected_db).await?;
                        let mut resp_keys = Vec::new();
                        for key in keys.iter() {
                            resp_keys.push(ResponseType::BulkString(key.as_bytes().to_vec()))
                        }
                        let resp = ResponseType::Array(resp_keys);
                        write_resp(&mut response_buff, &resp).await?;
                    }
                }
            }
        }

        Command::Info => {
            if !arguments.is_empty() {
                if let Some(category) = arguments[0].string() {
                    if category.to_lowercase() == "replication" {
                        let mut replication_info = String::new();
                        replication_info.push_str("# Replication\n");
                        if let Some(_replica_of) = CONFIG.read().await.replica_of.as_ref() {
                            replication_info.push_str("role:slave\n");
                        } else {
                            replication_info.push_str("role:master\n");
                            replication_info.push_str("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n");
                            replication_info.push_str("master_repl_offset:0\n");
                        }

                        write_bulk_string(&mut response_buff, replication_info.as_bytes())?;
                    }
                }
            }
        }
    }

    client.stream.write_all(response_buff.get_ref()).await?;
    client.stream.flush().await?;

    Ok(())
}

fn write_resp<'a>(buffer: &'a mut Writer<Vec<u8>>, value: &'a ResponseType)
    -> BoxFuture<'a, Result<(), anyhow::Error>> {
    Box::pin(async move {
        match value {
            ResponseType::Array(elements) => {
                write_array(buffer, elements).await?;
            }

            ResponseType::BulkString(s) => {
                write_bulk_string(buffer, s)?;
            }

            //_ => todo!("Need to implement writing {}", value)
        }

        Ok(())
    })
}

fn write_array<'a>(buffer: &'a mut Writer<Vec<u8>>, elements: &'a [ResponseType])
    -> BoxFuture<'a, Result<(), anyhow::Error>> {
    Box::pin(async move {
        buffer.write_all(format!("*{}\r\n", elements.len()).as_bytes())?;
        for e in elements.iter() {
            write_resp(buffer, e).await?;
        }
        Ok(())
    })
}
