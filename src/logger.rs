#[cfg(test)]
mod tests {
    fn init_logger() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::max())
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();
    }

    #[test]
    fn test_logger() {
        use log::{debug, error, info, log_enabled, Level};

        init_logger();

        debug!("this is a debug {}", "message");
        error!("this is printed by default");

        if log_enabled!(Level::Info) {
            let x = dbg!(3 * 4);
            // expensive computation
            info!("the answer was: {}", x);
        }
    }
}
