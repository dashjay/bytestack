use bst::utils;
use clap::{Parser, Subcommand};
use std::process::exit;
use tabled::Table;

use log::{error, info};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Sets a custom config file
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = utils::DEFAULT_CONFIG_PATH_TEMPLATE
    )]
    config_path: Option<String>,

    /// Turn debugging information on
    #[arg(short, long, default_value = "info")]
    log_level: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Stat try to list stacks under dir
    Stat { path: Option<String> },
    /// LS try to list all file in a stack
    LS { path: Option<String> },
    /// Get
    Get {
        /// index_id is given by ls, the unique way to access data, like 1,a90007cc79976
        #[arg(short = 'i', long = "index_id")]
        index_id: Option<String>,
        /// path: where to find stacks
        #[arg(long = "path")]
        path: Option<String>,
        /// target is where the file put
        #[arg(short = 't', long = "target", default_value = "-")]
        target: Option<String>,
        /// consistency issues are commonly solved by underlying storage, this check is closed usually
        #[arg(short = 'c', long = "check_crc", default_value = "false")]
        check_crc: Option<bool>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    utils::init_logger(&cli.log_level);

    let content = bst::utils::read_config_file(&cli.config_path);
    let cfg: bytestack::sdk::Config = toml::from_str(&content).unwrap();

    let handler = bytestack::sdk::Handler::new(cfg).await;
    match &cli.command {
        Commands::Stat { path } => {
            let path = match path {
                Some(p) => p,
                None => {
                    error!("<PATH> is needed");
                    exit(1);
                }
            };
            info!("run stat on {path:?}");
            let reader = handler.open_reader(path).unwrap();
            let out = match reader.list_al().await {
                Ok(stacks) => stacks,
                Err(e) => {
                    error!("list stack error: {}", e);
                    exit(1);
                }
            };
            let tbl = Table::new(out).to_string();
            println!("{}", tbl);
        }
        Commands::LS { path } => {
            let path = match path {
                Some(p) => p,
                None => {
                    error!("<PATH> is needed");
                    exit(1);
                }
            };

            let reader = handler.open_reader(path).unwrap();
            let stack_ids = match reader.list().await {
                Ok(res) => res,
                Err(e) => {
                    eprintln!("stat path {} error: {:?}", path, e);
                    exit(1)
                }
            };
            for stack_id in stack_ids {
                let res = match reader.list_stack(stack_id).await {
                    Ok(res) => res,
                    Err(e) => {
                        eprintln!("list stack {} error: {:?}", stack_id, e);
                        exit(1)
                    }
                };
                res.iter()
                    .for_each(|ir| println!("{},{}", stack_id, ir.index_id()))
            }
        }
        Commands::Get {
            path,
            index_id,
            target,
            check_crc,
        } => {
            todo!()
        }
    }
}
