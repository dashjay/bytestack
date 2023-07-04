use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;
use std::{env, process::exit};
use tabled::Table;

use log::{error, info, warn};

const DEFAULT_CONFIG_PATH_TEMPLATE: &str = "($HOME|%USERPROFILE%)/.config/bytestack/config.toml";
const DEFAULT_CONFIG_PATH: &str = ".config/bytestack/config.toml";

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Sets a custom config file
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = DEFAULT_CONFIG_PATH_TEMPLATE
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
    /// Adds files to myapp
    Stat { path: Option<String> },
    LS {
        /// path is all kind of path
        #[arg(short = 'p', long = "path")]
        path: Option<String>,
        #[arg(short = 'o', long = "output", value_enum)]
        output: Option<String>,
        #[arg(long = "output-path", default_value = "-")]
        output_path: Option<String>,
    },
    Get {
        /// index_id is given by ls, the unique way to access data, like 1,a90007cc79976
        #[arg(short = 'i', long = "index_id")]
        index_id: Option<String>,
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
    if let Some(level) = cli.log_level {
        let log_filter = match log::LevelFilter::from_str(level.as_str()) {
            Ok(l) => l,
            Err(e) => {
                let levels = log::Level::iter();
                let levels_collect: Vec<String> =
                    levels.map(|x| x.to_string().to_lowercase()).collect();
                panic!(
                    "unknown level: {}, parse error: {:?}, should in {:?}",
                    level, e, levels_collect,
                )
            }
        };
        log::set_logger(&bst::utils::STDOUT_LOG).unwrap();
        log::set_max_level(log_filter);
        info!("log::set_max_level: {}", log_filter.as_str().to_lowercase());
    }
    let content = match &cli.config_path {
        Some(path) => {
            let path = {
                if path == DEFAULT_CONFIG_PATH_TEMPLATE {
                    let home_dir = match env::var("HOME") {
                        Ok(dir) => dir,
                        Err(_) => match env::var("USERPROFILE") {
                            Ok(dir) => dir,
                            Err(_) => panic!("get env error"),
                        },
                    };
                    let home_dir = PathBuf::from(home_dir);
                    let full_path = home_dir.join(DEFAULT_CONFIG_PATH);
                    full_path
                } else {
                    PathBuf::from(path)
                }
            };
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(error) => panic!("open config file {:?} failed: {:?}", &path, error),
            };

            let mut content = String::new();
            match file.read_to_string(&mut content) {
                Ok(_) => content,
                Err(error) => {
                    panic!("read config file error {:?}", error);
                }
            }
        }
        None => {
            panic!("no config specified");
        }
    };
    let cfg: bytestack::sdk::Config = toml::from_str(&content).unwrap();
    let handler = bytestack::sdk::Handler::new(cfg);
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
            println!("{}",tbl);
        }
        Commands::LS {
            path,
            output,
            output_path,
        } => {
            info!("try to ls {path:?}")
        }
        Commands::Get {
            index_id,
            target,
            check_crc,
        } => {
            info!("try to get {index_id:?} to {target:?} with_crc_check:{check_crc:?}")
        }
    }
}
