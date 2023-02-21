use std::io::Error;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::Semaphore;

use crate::connection::Connection;
use crate::store::Store;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value = "1024")]
    conn_limit: u32,

    #[arg(short, long)]
    listen: String,

    #[arg(short = 'b', long, default_value = "1024")]
    listen_backlog: u32,

    // max_item_size: String,
    #[arg(short, long, default_value = "11211")]
    port: u32,

    #[arg(short, long)]
    runtimes: Option<u32>,
}

pub struct Server {
    cli: Cli,
    store: Arc<Store>,
}

impl Server {
    pub fn new(cli: Cli) -> Self {
        Self {
            cli,
            store: Arc::new(Store::new()),
        }
    }

    pub async fn run(&self) -> Result<(), Error> {
        let addr = format!("{}:{}", self.cli.listen, self.cli.port)
            .parse()
            .unwrap();
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.bind(addr)?;

        let tcp_listener = socket.listen(self.cli.listen_backlog)?;
        println!("server started, listening at {}", addr);

        let semaphore = Arc::new(Semaphore::new(self.cli.conn_limit as usize));

        loop {
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            match tcp_listener.accept().await {
                Ok((stream, _)) => {
                    let store = self.store.clone();
                    tokio::spawn(async move {
                        let mut conn = Connection::new(stream, store);
                        if let Err(e) = conn.process().await {
                            eprintln!("process err: {}", e);
                        }
                        drop(permit);
                    });
                }
                Err(e) => eprintln!("accept err: {}", e),
            }
            // println!("accepted, remain: {}", semaphore.available_permits());
        }
    }
}
