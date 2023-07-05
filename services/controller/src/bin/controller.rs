use proto::controller::controller_server::ControllerServer;
use controller::server::BytestackController;
use tonic::transport::Server;
use clap::Parser;
use mongodb::{Client, options::ClientOptions};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Sets a custom config file
    #[arg(
        short,
        long,
        value_name = "MONGODB_URI",
    )]
    mongo_uri: Option<String>,

    #[arg(
        long,
        value_name = "PORT",
        default_value = ":8080"
    )]
    bind: Option<String>,
    /// Turn debugging information on
    #[arg(short, long, default_value = "info")]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let addr = cli.bind.unwrap().parse()?;
    
    let mut client_options = ClientOptions::parse(cli.mongo_uri.unwrap()).await?;
    client_options.app_name = Some("bytestack_controller".to_string());
    let client = Client::with_options(client_options)?;
    
    let handler = BytestackController::new(client);

    Server::builder()
        .add_service(ControllerServer::new(handler))
        .serve(addr)
        .await?;

    Ok(())
}
