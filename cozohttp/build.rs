use static_files::resource_dir;

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=../jupyter/_output/jupyter-lite.json");
    resource_dir("../jupyter/_output").build()
}
