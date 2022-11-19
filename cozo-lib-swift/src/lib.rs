#[swift_bridge::bridge]
mod ffi {
    extern "Rust" {
        fn hello_rust(name: &str) -> String;
    }
}

fn hello_rust(name: &str) -> String {
    String::from(format!("Hello {} from Rust!", name))
}