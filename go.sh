cargo build --release
# time RUST_LOG=ns_train_generator=INFO target/release/ns-train-generator.exe out-100k 4 --data test_100k.csv
# time RUST_LOG=DEBUG target/release/ns-train-generator.exe out-100k 4 --data test_100k.csv
time RUST_LOG=ns_train_generator=INFO target/release/ns-train-generator.exe out 4 --data y2017.csv
#time RUST_LOG=DEBUG target/release/ns-train-generator.exe out.csv --data test.csv
