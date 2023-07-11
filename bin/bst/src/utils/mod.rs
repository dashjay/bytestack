mod log;
pub use self::log::init_logger;
use std::{env, fs::File, io::Read, path::PathBuf};

pub const DEFAULT_CONFIG_PATH_TEMPLATE: &str =
    "($HOME|%USERPROFILE%)/.config/bytestack/config.toml";
pub const DEFAULT_CONFIG_PATH: &str = ".config/bytestack/config.toml";

pub fn read_config_file(config_path: &Option<String>) -> String {
    let content = match config_path {
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
                    home_dir.join(DEFAULT_CONFIG_PATH)
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
    content
}
