use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/clonepack.proto");
    prost_build::compile_protos(&["proto/clonepack.proto"], &["proto/"])?;
    Ok(())
}
