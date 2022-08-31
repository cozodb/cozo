use static_files::resource_dir;

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=../cozowebui/build/asset-manifest.json");
    resource_dir("../cozowebui/build").build()
}
