use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);

        tokio::spawn(async move {
            handle_connection(stream).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0;512];
    loop {
        match stream.read(&mut buffer).await {
            Ok(_size) => {
                let response = "+PONG\r\n";
                let _wrote_bytes = stream.write(response.as_bytes()).await.unwrap();
                stream.flush().await.unwrap();
            }
            Err(_e) => {
                // client disconnected?
                break;
            }
        }
    }
}