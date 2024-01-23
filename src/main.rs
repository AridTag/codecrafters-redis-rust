use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use bytes::BufMut;
use std::io::Write;

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);

        tokio::spawn(async move {
            match handle_connection(stream).await {
                Ok(_) => { }
                Err(e) => println!("{:?}", e)
            };
        });
    }
}

#[derive(Debug)]
pub enum RespType {
    Error(String),
    SimpleString(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespType>),
    NullArray,
    NullBulkString,
}

#[derive(Error, Debug)]
pub enum RespProtocolError {
    #[error("Unknown error")]
    Unknown,

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
    buffer: [u8; 512],
    write_index: usize,
}

impl RedisClientConnection {
    pub const fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: [0u8; 512],
            write_index: 0,
        }
    }

    pub async fn read(&mut self) -> Result<RespType, anyhow::Error> {
        fn slide_window(buffer: &mut [u8; 512], start: usize, length: usize) {
            let (from, to) = buffer.split_at_mut(start);
            to[..length].copy_from_slice(&from[..length]);
        }

        loop {
            if self.write_index >= self.buffer.len() {
                return Err(RespProtocolError::MessageTooBig.into());
            }

            let bytes_read = self.stream.read(&mut self.buffer[self.write_index..]).await?;
            if bytes_read == 0 {
                continue;
            }

            self.write_index += bytes_read;
            let end_index = self.write_index;
            let request = Self::parse_resp(&self.buffer[0..end_index])?;
            if let Some(RespParseResult { request, consumed }) = request {
                println!("Received request: {:?}", request);
                slide_window(&mut self.buffer, consumed, self.write_index);
                self.write_index -= consumed;

                return Ok(request);
            }
        }
    }


    fn parse_resp(buffer: &[u8]) -> Result<Option<RespParseResult>, RespProtocolError> {
        {
            let debug_str = String::from_utf8_lossy(buffer);
            println!("Parsing {}", debug_str);
        }
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
                consumed
            }
        ))
    }

    fn get_next_part_end(buffer: &[u8]) -> Option<usize> {
        for i in 0..buffer.len() {
            if buffer[i] == b'\r' {
                if i + 1 < buffer.len() && buffer[i + 1] == b'\n' {
                    return Some(i + 1);
                }
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
                request: RespType::BulkString((&remainder[..length]).iter().cloned().collect()),
                consumed: length as usize,
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
                request: RespType::Array(elements),
            }
        ))
    }
}

struct RespParseResult {
    request: RespType,
    consumed: usize,
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
// *<number-of-elements>\r\n<element-1>...<element-n>

async fn handle_connection(stream: TcpStream) -> Result<(), anyhow::Error> {
    let mut client = RedisClientConnection::new(stream);
    loop {
        let request = client.read().await?;
        match request {
            RespType::Array(elements) => {
                if elements.len() > 0 {
                    if let RespType::BulkString(command) = &elements[0] {
                        let command = String::from_utf8_lossy(command).to_string();
                        handle_command(&mut client, command, &elements[1..]).await?;
                    }
                }
            }

            _ => todo!("Gotta implement")
        }
    }
}

async fn handle_command(client: &mut RedisClientConnection, command: String, arguments: &[RespType]) -> Result<(), anyhow::Error> {
    match command.as_str() {
        "ECHO" | "echo" => {
            if let RespType::BulkString(string) = &arguments[0] {
                let len_str = string.len().to_string();
                let mut buff = vec![].writer();
                buff.write(format!("${}\r\n{}\r\n", len_str, String::from_utf8_lossy(string)).as_bytes())?;

                client.stream.write(buff.get_ref()).await?;
                client.stream.flush().await?;
            }
        }

        "PING" | "ping" => {
            let mut buff = vec![].writer();
            buff.write(b"+PONG\r\n")?;
            client.stream.write(buff.get_ref()).await?;
            client.stream.flush().await?;
        }

        _ => todo!("Gotta implement")
    }

    Ok(())
}