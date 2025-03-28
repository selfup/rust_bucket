## Rust Bucket (work in progress)

![Rust Bucket](https://github.com/user-attachments/assets/ffcb8c10-ca38-4bb2-97dd-1663ccc42093)

### Goals
* Provide a simple JSON key-value store API
* Write to the filesystem for persistence
* Flexible structured or unstructured tables / records
* Can infer any data type with Serialize / Deserialize implementations via [Serde](https://github.com/serde-rs/serde)
* Works on stable!
* Synchronous performance
* Suitable for microservices
* Quick to setup

### Drawbacks
* No mmap
* Files don't lock, all usage should be synchronous
* Currently a library and not a server
* Project dependent
* Not yet on crates.io
* Performance is heavily influenced by SSD / HDD io speeds (but storage is cheap)

### Benchmarks

```
cargo bench

open target/criterion/report/index.html
```
