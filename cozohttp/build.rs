use static_files::resource_dir;

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=../cozo_webui/src/App.svelte");
    resource_dir("../cozo_webui/dist").build()
}
