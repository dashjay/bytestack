use bst::utils;
use clap::{Parser, Subcommand};
use log::{error, info};
use std::{fs::File, io::Write, process::exit};
use tabled::Table;

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

    /// Get fetch data from origin
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

    /// Bind stack-id to some source
    Bind {
        #[arg(long = "stack-id")]
        stack_id: Option<u64>,

        #[arg(long = "path")]
        path: Option<String>,

        #[arg(long = "cancel", default_value = "false")]
        cancel: Option<bool>,
    },

    /// Preload create task for bserver to preload the dataset.
    Preload {
        /// index_id is given by ls, the unique way to access data, like 1,a90007cc79976
        #[arg(long = "stack-id")]
        stack_id: Option<u64>,

        /// path: where to find the stack
        #[arg(long = "replicas", default_value = "1")]
        replicas: Option<i64>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    bytestack::utils::init_logger(&cli.log_level);

    let content = bst::utils::read_config_file(&cli.config_path);
    let cfg: bytestack::sdk::Config = toml::from_str(&content).unwrap();

    let mut handler = bytestack::sdk::Handler::new(cfg).await;
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
            let index_id = match index_id {
                Some(id) => id,
                None => {
                    error!("index_id is needed");
                    exit(1);
                }
            };
            let path = match path {
                Some(p) => p,
                None => {
                    error!("path is needed");
                    exit(1);
                }
            };
            let reader = handler.open_reader(path).unwrap();
            let data = match reader.fetch(index_id, check_crc.unwrap()).await {
                Ok(res) => res,
                Err(e) => {
                    error!("fetch {} error {:?}", index_id, e);
                    exit(1);
                }
            };
            let target = match target {
                Some(t) => t,
                None => "-",
            };
            if target == "-" {
                use std::io::{self, Write};
                let mut stdout = io::stdout().lock();
                let _ = stdout.write_all(&data);
            } else {
                let mut fd = File::create(target).unwrap();
                let _ = fd.write(&data);
            }
        }
        Commands::Bind {
            stack_id,
            path,
            cancel,
        } => {
            let stack_id = match stack_id {
                Some(id) => *id,
                None => {
                    error!("stack_id is needed");
                    exit(1);
                }
            };
            let path = match path {
                Some(path) => path,
                None => {
                    error!("path is needed");
                    exit(1);
                }
            };
            if cancel.unwrap() {
                let _resp = match handler.unbind_stack(stack_id, path).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("bind {} to {} error: {:?}", stack_id, path, e);
                        exit(1);
                    }
                };
            } else {
                let _resp = match handler.bind_stack(stack_id, path).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("bind {} to {} error: {:?}", stack_id, path, e);
                        exit(1);
                    }
                };
            }
        }
        Commands::Preload { stack_id, replicas } => {
            let stack_id = match stack_id {
                Some(id) => *id,
                None => {
                    error!("stack_id is needed");
                    exit(1);
                }
            };

            match handler
                .preload(stack_id, replicas.unwrap().to_owned())
                .await
            {
                Ok(resp) => {
                    for i in resp.preloads {
                        println!("{:?}", i)
                    }
                }
                Err(e) => {
                    error!("preload {} error: {:?}", stack_id, e);
                    exit(1);
                }
            };
        }
    }
}
