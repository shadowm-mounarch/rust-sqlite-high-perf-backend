use crate::db::concurrency::TransactionManager;
use crate::db::storage::{Table, TableStorage};
use crate::db::types::TableSchema;
use dashmap::DashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct DbEngine {
    dir_path: PathBuf,
    tables: DashMap<String, Arc<Table>>,
    tx_manager: Arc<TransactionManager>,
}

impl DbEngine {
    pub fn new<P: AsRef<Path>>(dir_path: P) -> io::Result<Self> {
        let path = dir_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let tables = DashMap::new();

        // Load existing tables
        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.flatten() {
                if entry.file_type()?.is_dir() {
                    let table_name = entry.file_name().to_string_lossy().to_string();
                    if let Ok(storage) = TableStorage::open(&path, &table_name) {
                        let table = Table::new(storage);
                        tables.insert(table_name, Arc::new(table));
                    }
                }
            }
        }

        Ok(Self {
            dir_path: path,
            tables,
            tx_manager: Arc::new(TransactionManager::new()),
        })
    }

    pub fn create_table(&self, schema: TableSchema) -> io::Result<()> {
        let name = schema.name.clone();
        let storage = TableStorage::create(&self.dir_path, schema)?;
        let table = Table::new(storage);
        self.tables.insert(name, Arc::new(table));
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        self.tables.get(name).map(|t| Arc::clone(t.value()))
    }

    pub fn tx_manager(&self) -> Arc<TransactionManager> {
        Arc::clone(&self.tx_manager)
    }
}
