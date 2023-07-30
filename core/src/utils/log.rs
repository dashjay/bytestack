use std::str::FromStr;

use log::{info, LevelFilter, Metadata, Record};

pub static mut STDOUT_LOG: SimpleLogger = SimpleLogger {
    level: LevelFilter::Info,
};

pub struct SimpleLogger {
    level: LevelFilter,
}

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "[{}] - target: {} - {}",
                record.level(),
                record.target(),
                record.args()
            );
        }
    }
    fn flush(&self) {}
}

/// init_logger simply set logger output to stdout and logger level
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
        unsafe {
            STDOUT_LOG.level = log_filter;
            log::set_max_level(log_filter);
            log::set_logger(&STDOUT_LOG).unwrap();
        }
        info!("log::set_max_level: {}", log_filter.as_str().to_lowercase());
    }
}
