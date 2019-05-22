cargo build
#time RUST_LOG=ns_train_generator=DEBUG target/release/ns-train-generator.exe out.csv --data test.csv
#time RUST_LOG=DEBUG target/release/ns-train-generator.exe out.csv --data test.csv
time RUST_BACKTRACE=1 target/debug/ns-train-generator.exe out.csv --data test.csv
