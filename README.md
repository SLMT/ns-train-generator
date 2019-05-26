# Neural Storage Training Data Generator

## 使用方式

### 編譯

首先先[安裝 Rust][1]，然後在此資料夾內執行以下指令：

```bash
> cargo build --release
```

程式會產生在 `target/release/ns-train-generator.exe`。

### 設定

使用前記得先修改 `config.toml` 的內容進行設定：

```toml
# 資料庫相關設定
[db]
username = "yicchen"
password = "datalab"
db_name = "yicchen"
table_name = "y2017_16"
port = "5433"
host = "localhost"

# 產生資料時要使用的 field 編號
[generator.fields]
agg_fields = [0]
select_fields = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
group_fields = [] # 以 select_fields 內的順序為準

# 產生資料時的 Normal Distribution 參數，注意 means 跟 std_devs 的 shape 要相同
[generator.normal]
means = [[1, 2, 40, 25, 65, 65, 1, 125, 16, 120, 225, 250, 1, 3, 8, 6],
         [0, 0, 10,  5, 10, 10, 1,  10,  3,  10,  20,  20, 0, 0, 1, 1]]
std_devs = [[1, 1, 10,  6, 16, 16, 1, 30,  4,  30,  56,  62, 1, 1, 2, 2],
            [1, 1, 10,  6, 16, 16, 1, 30,  4,  30,  56,  62, 1, 1, 2, 2]]
```

### 使用方式

使用說明如下：

```
NS Train Generator 1.0
Yu-Shan Lin <yslin@datalab.cs.nthu.edu.tw>
The generator that generates the training data set for neural storage project.

USAGE:
    ns-train-generator.exe [OPTIONS] <OUTPUT FILE PREFIX> <# OF THREADS>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --config <CONFIG FILE>    Sets the path to a config file
    -d, --data <DATA FILE>        Sets the path to the input data file

ARGS:
    <OUTPUT FILE PREFIX>    Sets the prefix name/path of the output data file
    <# OF THREADS>          Sets the number of threads generating training data set
```

### 使用範例

例如要使用 4 個 thread，並且將最後產生的檔案以 `out` 最為開頭命名：

```bash
> ns-train-generator.exe out 4
```

如果想要看到訊息，要在最前面加上 `RUST_LOG=ns_train_generator=INFO`：

```bash
> RUST_LOG=ns_train_generator=INFO ns-train-generator.exe out 4
```

如果資料庫尚未有資料，想要將資料讀取進來的話，可以加上 `--data <DATA FILE>`，例如我想要讀取 `test.csv`：

```bash
> RUST_LOG=ns_train_generator=INFO ns-train-generator.exe out 4 --data test.csv
```

## 筆記

### 使用機器

CPU: Intel(R) Xeon(R) CPU E3-1231 v3
RAM: 16 GB
OS: Windows 10 64-bit Enterprise

### 1K 資料

- 讀取資料檔案：3 秒
- 存入 1M 資料到 DB：2 分 7 秒
- 從 DB 讀取資料：3 秒

### 100K 資料

- 全部跑完(包含將資料放入 DB 以及產生資料)：8 分 47 秒

[1]: https://rustup.rs/