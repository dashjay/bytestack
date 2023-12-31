use bytestack::utils::init_logger;
use clap::Parser;
use controller::server::BytestackController;
use log::info;
use mongodb::{options::ClientOptions, Client};
use proto::controller::controller_server::ControllerServer;
use tonic::transport::Server;
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "MONGODB_URI")]
    mongo_uri: Option<String>,

    #[arg(long, value_name = "PORT", default_value = "0.0.0.0:8080")]
    bind: Option<String>,
    /// Turn debugging information on
    #[arg(short, long, default_value = "info")]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    init_logger(&cli.log_level);
    let addr = cli.bind.unwrap().parse()?;

    let mut client_options = ClientOptions::parse(cli.mongo_uri.unwrap()).await?;
    client_options.app_name = Some("bytestack_controller".to_string());
    let client = Client::with_options(client_options)?;

    let handler = BytestackController::new(client);
    info!("bytestack_controller running on {:?}", &addr);
    Server::builder()
        .add_service(ControllerServer::new(handler))
        .serve(addr)
        .await?;

    Ok(())
}
