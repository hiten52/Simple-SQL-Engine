use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;

/// Represents a row in a database table.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Row {
    pub id: u128,
    pub columns: BTreeMap<String, Value>,
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Represents a database table, which contains a set of rows.
#[derive(Debug)]
pub struct Table {
    pub rows: BTreeSet<Row>,
}

impl Table {
    /// Creates a new, empty table.
    pub fn new() -> Self {
        Table {
            rows: BTreeSet::new(),
        }
    }

    /// Adds a row to the table.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the row.
    /// * `columns` - A map of column names to their values.
    pub fn add_row(&mut self, id: u128, columns: BTreeMap<String, Value>) {
        self.rows.insert(Row { id, columns });
    }
}

/// Represents a database, which contains multiple tables.
#[derive(Debug)]
pub struct Database {
    pub tables: BTreeMap<String, Table>,
}

impl Database {
    /// Creates a new, empty database.
    pub fn new() -> Self {
        Database {
            tables: BTreeMap::new(),
        }
    }

    /// Creates a new, empty table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table.
    fn create_table(&mut self) -> Table {
        Table::new()
    }

    /// Inserts a table into the database.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table.
    /// * `table` - The table to insert.
    fn insert_table(&mut self, table_name: String, table: Table) {
        self.tables.insert(table_name, table);
    }
}

/// Loads a database from a JSON file.
///
/// # Arguments
///
/// * `file_path` - The path to the JSON file.
///
/// # Returns
///
/// A `Result` containing the loaded database or an I/O error.
pub fn load_database(file_path: &str) -> Result<Database, io::Error> {
    let data = fs::read_to_string(file_path)?;
    let data: Value = serde_json::from_str(&data).expect("Failed to parse JSON.");

    let mut db = Database::new();

    if let Some(tables) = data.as_object() {
        for (table_name, rows) in tables {
            let mut table = db.create_table();

            if let Some(rows_array) = rows.as_array() {
                for row in rows_array {
                    if let Some(row_object) = row.as_object() {
                        if let Some(id) = row_object.get("id").and_then(Value::as_u64) {
                            let columns = row_object
                                .iter()
                                .filter(|&(k, _)| k != "id")
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();

                            table.add_row(id as u128, columns);
                        }
                    }
                }
            }

            db.insert_table(table_name.clone(), table);
        }
    }

    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests loading a database with a correct file name.
    #[test]
    fn correct_file_name() {
        let file_name = "database/movie_data.json";
        load_database(file_name).unwrap();
    }

    /// Tests loading a database with an incorrect file name.
    ///
    /// This test should panic.
    #[test]
    #[should_panic]
    fn incorrect_file_name() {
        let file_name = "";
        load_database(file_name).unwrap();
    }
}
