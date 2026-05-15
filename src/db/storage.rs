use crate::db::types::{TableSchema, Value};
use bincode::{deserialize, serialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};

pub const TOMBSTONE_ALIVE: u8 = 0;
pub const TOMBSTONE_DELETED: u8 = 1;

#[derive(Debug, Clone)]
pub struct Row {
    pub is_deleted: bool,
    pub timestamp: u64,
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(timestamp: u64, values: Vec<Value>) -> Self {
        Self {
            is_deleted: false,
            timestamp,
            values,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        let tombstone = if self.is_deleted {
            TOMBSTONE_DELETED
        } else {
            TOMBSTONE_ALIVE
        };
        buffer.push(tombstone);

        buffer.write_u64::<LittleEndian>(self.timestamp).unwrap();

        let values_bytes = serialize(&self.values).unwrap();
        buffer.extend(values_bytes);

        // Construct final payload with length header
        let mut final_payload = Vec::new();
        final_payload
            .write_u32::<LittleEndian>(buffer.len() as u32)
            .unwrap();
        final_payload.extend(buffer);

        final_payload
    }

    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 9 {
            return None;
        }

        let mut cursor = Cursor::new(bytes);
        let tombstone = cursor.read_u8().unwrap();
        let timestamp = cursor.read_u64::<LittleEndian>().unwrap();

        let values: Vec<Value> = deserialize(&bytes[9..]).unwrap();

        Some(Self {
            is_deleted: tombstone == TOMBSTONE_DELETED,
            timestamp,
            values,
        })
    }
}

pub struct TableStorage {
    dir_path: PathBuf,
    #[allow(dead_code)]
    name: String,
    schema: TableSchema,
    data_file: File,
}

impl TableStorage {
    pub fn open(dir_path: &Path, name: &str) -> io::Result<Self> {
        let table_dir = dir_path.join(name);
        std::fs::create_dir_all(&table_dir)?;

        let meta_path = table_dir.join(format!("{}.meta", name));
        let data_path = table_dir.join("data.bin");

        let schema = if meta_path.exists() {
            let meta_data = std::fs::read(&meta_path)?;
            serde_json::from_slice(&meta_data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        } else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Schema not found"));
        };

        let data_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&data_path)?;

        Ok(Self {
            dir_path: table_dir,
            name: name.to_string(),
            schema,
            data_file,
        })
    }

    pub fn create(dir_path: &Path, schema: TableSchema) -> io::Result<Self> {
        let table_dir = dir_path.join(&schema.name);
        std::fs::create_dir_all(&table_dir)?;

        let meta_path = table_dir.join(format!("{}.meta", schema.name));
        let meta_data = serde_json::to_vec(&schema).unwrap();
        std::fs::write(&meta_path, meta_data)?;

        let data_path = table_dir.join("data.bin");
        let data_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&data_path)?;

        Ok(Self {
            dir_path: table_dir,
            name: schema.name.clone(),
            schema,
            data_file,
        })
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

use crossbeam_channel::{unbounded, Sender};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Table {
    storage: Arc<RwLock<TableStorage>>,
    write_queue: Sender<Vec<u8>>,
}

impl Table {
    pub fn new(storage: TableStorage) -> Self {
        let storage = Arc::new(RwLock::new(storage));

        let (tx, rx) = unbounded::<Vec<u8>>();
        let storage_clone = Arc::clone(&storage);

        std::thread::spawn(move || {
            let mut batch = Vec::new();
            let mut batch_count = 0;

            while let Ok(row_bytes) = rx.recv() {
                batch.extend(row_bytes);
                batch_count += 1;

                // Flush every 1000 rows or when channel is empty
                if batch_count >= 1000 || rx.is_empty() {
                    let mut s = storage_clone.write();
                    s.data_file.write_all(&batch).unwrap();
                    s.data_file.sync_data().unwrap();
                    batch.clear();
                    batch_count = 0;
                }
            }
        });

        Self {
            storage,
            write_queue: tx,
        }
    }

    pub fn insert(&self, row: Row) -> io::Result<()> {
        let bytes = row.serialize();
        self.write_queue.send(bytes).unwrap();
        Ok(())
    }

    pub fn scan(&self, read_ts: u64) -> io::Result<Vec<Row>> {
        let s = self.storage.read();
        let mut file = File::open(s.dir_path.join("data.bin")).unwrap();

        let mut rows = Vec::new();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut cursor = Cursor::new(&buffer);
        while (cursor.position() as usize) < buffer.len() {
            let len = match cursor.read_u32::<LittleEndian>() {
                Ok(l) => l,
                Err(_) => break, // EOF
            };

            let pos = cursor.position() as usize;
            if pos + len as usize > buffer.len() {
                break; // Incomplete row
            }

            let row_bytes = &buffer[pos..pos + len as usize];
            if let Some(row) = Row::deserialize(row_bytes) {
                if row.timestamp <= read_ts && !row.is_deleted {
                    rows.push(row);
                }
            }

            cursor.set_position(cursor.position() + len as u64);
        }

        Ok(rows)
    }
}
