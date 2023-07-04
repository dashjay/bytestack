use clap::{Arg, Command};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
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
        #[arg(short = 'o', long = "output")]
        output: Option<String>,
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
    match &cli.command {
        Commands::Stat { path } => {
            println!("try to stat {path:?}")
        }
        Commands::LS { path, output } => {
            println!("try to ls {path:?}")
        }
        Commands::Get {
            index_id,
            target,
            check_crc,
        } => {
            println!("try to get {index_id:?} to {target:?} with_crc_check:{check_crc:?}")
        }
    }
}
