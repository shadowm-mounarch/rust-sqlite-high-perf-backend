use high_perf_backend::db::engine::DbEngine;
use high_perf_backend::db::storage::Row;
use high_perf_backend::db::types::{ColumnDef, DataType, TableSchema, Value};
use std::time::Instant;

fn main() {
    let _ = std::fs::remove_dir_all("bench_data");
    let db = DbEngine::new("bench_data").unwrap();

    let schema = TableSchema::new(
        "bench_table".to_string(),
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
                primary_key: true,
            },
            ColumnDef {
                name: "data".to_string(),
                data_type: DataType::String,
                primary_key: false,
            },
        ],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table("bench_table").unwrap();
    let num_inserts = 100_000;

    let start = Instant::now();
    for i in 0..num_inserts {
        let row = Row::new(
            i as u64,
            vec![
                Value::Int(i),
                Value::String("benchmark_data_string".to_string()),
            ],
        );
        table.insert(row).unwrap();
    }

    let duration = start.elapsed();
    println!("Inserted {} rows in {:?}", num_inserts, duration);
    println!(
        "Writes per second: {}",
        (num_inserts as f64 / duration.as_secs_f64()) as u64
    );
}
