fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_server(true)
        .compile_protos(
            &[
                "../../protocol/commerce/fraud_detection.proto",
                "../../protocol/commerce/order_query.proto",
            ],
            &["../../protocol/commerce"],
        )?;
    println!("cargo:rerun-if-changed=../../protocol/commerce/fraud_detection.proto");
    println!("cargo:rerun-if-changed=../../protocol/commerce/order_query.proto");
    Ok(())
}
