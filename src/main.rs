use clap::Parser;
use mcd::server::{Cli, Server};
use std::io::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    let server = Server::new(cli);
    server.run().await?;
    Ok(())
}
