use crate::db::engine::DbEngine;
use crate::db::storage::Row;
use crate::db::types::{ColumnDef, DataType, TableSchema, Value};
use sqlparser::ast::{self, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

pub struct SqlEngine {
    db: Arc<DbEngine>,
}

impl SqlEngine {
    pub fn new(db: Arc<DbEngine>) -> Self {
        Self { db }
    }

    pub fn execute(&self, sql: &str) -> Result<Vec<Row>, String> {
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|e| format!("Parse error: {:?}", e))?;

        if ast.is_empty() {
            return Ok(Vec::new());
        }

        match &ast[0] {
            Statement::CreateTable { name, columns, .. } => {
                self.execute_create_table(name, columns)
            }
            Statement::Insert {
                table_name, source, ..
            } => self.execute_insert(table_name, source),
            Statement::Query(query) => self.execute_select(query),
            _ => Err("Unsupported statement".to_string()),
        }
    }

    fn execute_create_table(
        &self,
        name: &ast::ObjectName,
        columns: &[ast::ColumnDef],
    ) -> Result<Vec<Row>, String> {
        let table_name = name.to_string();
        let mut defs = Vec::new();

        for col in columns {
            let col_name = col.name.value.clone();
            let data_type = match &col.data_type {
                ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::BigInt(_) => {
                    DataType::Int
                }
                ast::DataType::Text | ast::DataType::String(_) | ast::DataType::Varchar(_) => {
                    DataType::String
                }
                ast::DataType::Float(_) | ast::DataType::Real => DataType::Float,
                ast::DataType::Boolean => DataType::Bool,
                ast::DataType::JSON => DataType::Json,
                _ => return Err(format!("Unsupported type: {:?}", col.data_type)),
            };

            let is_primary = col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    ast::ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            });

            defs.push(ColumnDef {
                name: col_name,
                data_type,
                primary_key: is_primary,
            });
        }

        let schema = TableSchema::new(table_name.clone(), defs);
        self.db.create_table(schema).map_err(|e| e.to_string())?;
        Ok(Vec::new())
    }

    fn execute_insert(
        &self,
        table_name: &ast::ObjectName,
        source: &Option<Box<ast::Query>>,
    ) -> Result<Vec<Row>, String> {
        let table_name_str = table_name.to_string();
        let table = self
            .db
            .get_table(&table_name_str)
            .ok_or_else(|| "Table not found".to_string())?;

        let source = source.as_ref().ok_or("Insert source missing")?;
        let body = match &*source.body {
            ast::SetExpr::Values(values) => values,
            _ => return Err("Only INSERT VALUES is supported".to_string()),
        };

        let tx_ts = self.db.tx_manager().begin_write();

        for row_ast in &body.rows {
            let mut values = Vec::new();
            for expr in row_ast {
                let val = match expr {
                    ast::Expr::Value(val) => match val {
                        ast::Value::Number(n, _) => {
                            if n.contains('.') {
                                Value::Float(n.parse().unwrap_or(0.0))
                            } else {
                                Value::Int(n.parse().unwrap_or(0))
                            }
                        }
                        ast::Value::SingleQuotedString(s) => Value::String(s.clone()),
                        ast::Value::Boolean(b) => Value::Bool(*b),
                        ast::Value::Null => Value::Null,
                        _ => return Err(format!("Unsupported value expression: {:?}", expr)),
                    },
                    ast::Expr::Identifier(ident) => Value::String(ident.value.clone()),
                    _ => return Err(format!("Unsupported value expression: {:?}", expr)),
                };
                values.push(val);
            }

            let row = Row::new(tx_ts, values);
            table.insert(row).map_err(|e| e.to_string())?;
        }

        Ok(Vec::new())
    }

    fn execute_select(&self, query: &ast::Query) -> Result<Vec<Row>, String> {
        let select = match &*query.body {
            ast::SetExpr::Select(select) => select,
            _ => return Err("Only basic SELECT is supported".to_string()),
        };

        if select.from.is_empty() {
            return Err("FROM clause is missing".to_string());
        }

        let table_name = match &select.from[0].relation {
            ast::TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err("Unsupported FROM clause".to_string()),
        };

        let table = self
            .db
            .get_table(&table_name)
            .ok_or_else(|| "Table not found".to_string())?;
        let read_ts = self.db.tx_manager().begin_read();

        let all_rows = table.scan(read_ts).map_err(|e| e.to_string())?;

        if let Some(_selection) = &select.selection {
            let mut filtered = Vec::new();
            for row in all_rows {
                filtered.push(row);
            }
            return Ok(filtered);
        }

        Ok(all_rows)
    }
}
