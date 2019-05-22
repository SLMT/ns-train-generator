cargo build --release
#time RUST_LOG=ns_train_generator=DEBUG target/release/ns-train-generator.exe out.csv --data test.csv
time RUST_LOG=ns_train_generator=DEBUG target/release/ns-train-generator.exe out.csv --data y2017.csv
#time RUST_LOG=DEBUG target/release/ns-train-generator.exe out.csv --data test.csv
