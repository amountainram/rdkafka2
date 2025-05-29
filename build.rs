use cargo_metadata::MetadataCommand;
use std::{fs, path::Path};

fn copy_librdkafka_suppressions() {
    let metadata = MetadataCommand::new()
        .exec()
        .expect("Failed to read Cargo metadata");
    let rdkafka2_sys_src_dir = metadata
        .packages
        .iter()
        .find(|p| p.name.as_str() == "rdkafka2-sys")
        .expect("Dependency rdkafka2-sys not found")
        .manifest_path
        .parent()
        .expect("No parent for manifest path");

    let src_file = rdkafka2_sys_src_dir
        .join("librdkafka")
        .join("tests")
        .join("librdkafka.suppressions");
    let dest_file = Path::new("tests")
        .join("suppressions")
        .join("librdkafka.supp");

    if let Some(parent) = dest_file.parent() {
        fs::create_dir_all(parent).expect("Failed to create tests/suppressions directory");
    }

    fs::copy(&src_file, &dest_file).expect("Failed to copy suppression file");

    println!("cargo:rerun-if-changed={src_file}");
}

fn main() {
    copy_librdkafka_suppressions();
}
