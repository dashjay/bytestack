use std::str::FromStr;

use log::{info, Level, LevelFilter, Metadata, Record};

pub static STDOUT_LOG: SimpleLogger = SimpleLogger;

pub struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
}

pub fn init_logger(lvl: &Option<String>) {
    if let Some(level) = lvl {
        let level_str = level.to_string();
        let log_filter = match LevelFilter::from_str(&level_str) {
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
        log::set_logger(&STDOUT_LOG).unwrap();
        log::set_max_level(log_filter);
        info!("log::set_max_level: {}", log_filter.as_str().to_lowercase());
    }
}
