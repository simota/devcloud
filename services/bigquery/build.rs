fn main() {
    let proto = "proto/storage.proto";
    tonic_build::configure()
        .compile_protos(&[proto], &["proto"])
        .expect("compile BigQuery Storage API proto");
    println!("cargo:rerun-if-changed={proto}");
}
