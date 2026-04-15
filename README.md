# 🚀 High-Perf Rust & SQLite Backend

A "Steve Jobs" approach to high-performance web services: Minimal, elegant, and insanely fast. This backend is engineered to handle **100,000+ Requests Per Second (RPS)** on consumer-grade hardware (like a Lenovo IdeaPad) using Rust and a finely-tuned SQLite database.

## 📊 Performance Benchmark
Tested on: **Intel Core i5-13420H (12 threads) | 16GB RAM | Windows 11**

| Metric | Result |
| :--- | :--- |
| **Throughput** | **106,189 Requests/sec** |
| **Success Rate** | 100% |
| **Avg Latency** | 0.93ms |
| **99th Percentile** | 2.07ms |

## 🛠️ The "Turbo" Stack
- **Framework:** [Axum](https://github.com/tokio-rs/axum) (Built on Tokio & Tower).
- **Database:** SQLite with [deadpool-sqlite](https://github.com/bikeshedder/deadpool) for connection pooling.
- **Memory Allocator:** [mimalloc](https://github.com/microsoft/mimalloc) (Microsoft's high-performance allocator) for low-latency on Windows.
- **Serialization:** [sonic-rs](https://github.com/cloudwego/sonic-rs) (SIMD-accelerated JSON) utilizing AVX2 instructions.

## ⚡ SQLite "Death Mode" Configuration
To bypass traditional SQLite bottlenecks, the following PRAGMAs are applied:
- `WAL Mode`: Enables concurrent readers while writing.
- `Synchronous = NORMAL`: Drastically reduces disk sync overhead while maintaining safety.
- `Memory Mapping (mmap)`: Maps the DB file into RAM to avoid expensive system calls.
- `Page Size (4096)`: Optimized for modern SSD clusters.

## 🚀 Getting Started

### Prerequisites
- [Rust Toolchain](https://rustup.rs/)
- `oha` (for benchmarking): `cargo install oha`

### Running the Server
```powershell
# Compile with native CPU optimizations
$env:RUSTFLAGS="-C target-cpu=native"
cargo run --release
```

### Benchmarking
```powershell
oha -n 100000 -c 100 http://127.0.0.1:3000/health
```

## 🏗️ Project Structure
- `src/main.rs`: High-performance Axum routes and SQLite pool initialization.
- `Cargo.toml`: Optimized release profile (LTO, codegen-units=1, panic=abort).

## 📄 License
Open-source under the MIT License.
