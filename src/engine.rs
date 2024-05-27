use crate::database::Database;
use crate::parser::{Column, Comparison, Const, Query, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Represents a view of the database that is generated from executing a parsed SQL query.
#[derive(Debug)]
pub struct View<'a> {
    /// A vector of rows, where each row is represented as a `BTreeMap` of column names to values.
    pub rows: Vec<BTreeMap<String, Value>>,
    /// The parsed SQL query.
    parsed_query: Query<'a>,
    /// The database on which the query is executed.
    database: Database,
}

impl<'a> View<'a> {
    /// Executes a parsed SQL query on a database and returns a `View` object.
    ///
    /// # Arguments
    ///
    /// * `parsed_query` - The parsed SQL query.
    /// * `database` - The database to execute the query on.
    ///
    /// # Returns
    ///
    /// A `View` object containing the result of the query.
    pub fn execute(parsed_query: Query<'a>, database: Database) -> View<'a> {
        let view = View {
            rows: vec![],
            parsed_query,
            database,
        };

        view.from().joins().apply_where().select()
    }

    /// Populates the view with rows from the specified table in the `FROM` clause.
    ///
    /// # Returns
    ///
    /// A `View` object with rows from the specified table.
    fn from(self) -> View<'a> {
        let table_name = &self.parsed_query.from;

        View {
            rows: self.table_to_vec(table_name.to_owned()),
            parsed_query: self.parsed_query,
            database: self.database,
        }
    }

    /// Processes the `JOIN` clauses and merges rows from the joined tables.
    ///
    /// # Returns
    ///
    /// A `View` object with rows merged according to the `JOIN` clauses.
    fn joins(self) -> View<'a> {
        let mut rows = self.rows.clone();

        for join in &self.parsed_query.joins {
            let mut new_rows: Vec<BTreeMap<String, Value>> = vec![];
            let join_table = self.table_to_vec(join.table_name.clone());

            for row in &rows {
                for join_row in &join_table {
                    let left_value = self
                        .get_column_value(row, &join.on.left)
                        .or(self.get_column_value(join_row, &join.on.left))
                        .unwrap();
                    let right_value = self
                        .get_column_value(row, &join.on.right)
                        .or(self.get_column_value(join_row, &join.on.right))
                        .unwrap();

                    if self.compare_values(&left_value, &join.on.comparison, &right_value) {
                        let mut new_row = row.clone();
                        for (k, v) in join_row {
                            new_row.insert(k.clone(), v.clone());
                        }
                        new_rows.push(new_row);
                    }
                }
            }

            rows = new_rows;
        }

        View {
            rows,
            parsed_query: self.parsed_query,
            database: self.database,
        }
    }

    /// Filters rows based on the `WHERE` clause.
    ///
    /// # Returns
    ///
    /// A `View` object with rows filtered according to the `WHERE` clause.
    fn apply_where(self) -> View<'a> {
        let mut rows = self.rows.clone();
        if let Some(where_clause) = &self.parsed_query.where_clause {
            rows.retain(|row| {
                where_clause.left.get_const();
                let left_value = self.get_column_value(row, &where_clause.left).unwrap();
                let right_value = self.get_column_value(row, &where_clause.right).unwrap();
                self.compare_values(&left_value, &where_clause.comparison, &right_value)
            });
        }
        View {
            rows,
            parsed_query: self.parsed_query,
            database: self.database,
        }
    }

    /// Selects the specified columns and constructs the final result set.
    ///
    /// # Returns
    ///
    /// A `View` object with the selected columns.
    fn select(self) -> View<'a> {
        let mut selected_rows: Vec<BTreeMap<String, Value>> = vec![];

        let column_names: BTreeSet<String> = self
            .parsed_query
            .select
            .iter()
            .map(|c| format!("{}.{}", c.table_name, c.column_name))
            .collect();

        selected_rows = self
            .rows
            .into_iter()
            .map(|x| {
                x.into_iter()
                    .filter(|(k, _)| column_names.contains(k))
                    .collect()
            })
            .collect();

        View {
            rows: selected_rows,
            parsed_query: self.parsed_query,
            database: self.database,
        }
    }

    /// Gets the value of a column in a row.
    ///
    /// # Arguments
    ///
    /// * `row` - A reference to the row.
    /// * `value` - The column value.
    ///
    /// # Returns
    ///
    /// An `Option` containing the value if it exists, otherwise `None`.
    fn get_column_value(&self, row: &BTreeMap<String, Value>, value: &Value) -> Option<Const> {
        if let Value::Const(c) = value {
            return Some(c.clone());
        };

        let table_name = value.get_table_name();
        let column_name = value.get_column_name();

        let key = &format!("{}.{}", table_name, column_name);
        match row.get(key) {
            None => None,
            value => value.unwrap().get_const(),
        }
    }

    /// Compares two values based on the specified comparison operator.
    ///
    /// # Arguments
    ///
    /// * `left` - The left value to compare.
    /// * `comparison` - The comparison operator.
    /// * `right` - The right value to compare.
    ///
    /// # Returns
    ///
    /// A boolean indicating the result of the comparison.
    fn compare_values(&self, left: &Const, comparison: &Comparison, right: &Const) -> bool {
        match (left, right) {
            (Const::Number(left), Const::Number(right)) => match comparison {
                Comparison::Eq => left == right,
                Comparison::Gt => left > right,
                Comparison::Lt => left < right,
                Comparison::Le => left <= right,
                Comparison::Ge => left >= right,
                Comparison::Ne => left != right,
            },
            (Const::String(left), Const::String(right)) => match comparison {
                Comparison::Eq => left == right,
                Comparison::Gt => left > right,
                Comparison::Lt => left < right,
                Comparison::Le => left <= right,
                Comparison::Ge => left >= right,
                Comparison::Ne => left != right,
            },
            _ => false,
        }
    }

    /// Converts a table to a vector of rows, each row being a `BTreeMap` of column names and values.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table.
    ///
    /// # Returns
    ///
    /// A vector of rows.
    fn table_to_vec(&self, table_name: String) -> Vec<BTreeMap<String, Value>> {
        let table = self.database.tables.get(&table_name).unwrap();
        table
            .rows
            .iter()
            .map(|row| {
                let mut columns: BTreeMap<String, Value> = row
                    .columns
                    .iter()
                    .map(|(k, v)| {
                        let v = Value::from_serde_value(v);
                        (format!("{}.{}", table_name, k), v)
                    })
                    .collect();
                columns.insert(
                    format!("{table_name}.id"),
                    Value::Const(Const::Number(row.id as i64)),
                );

                columns
            })
            .collect()
    }
}

impl Const {
    /// Converts a constant value to a string.
    ///
    /// # Returns
    ///
    /// A string representation of the constant value.
    fn to_string(&self) -> String {
        match self {
            Const::Number(n) => n.to_string(),
            Const::String(s) => s.clone(),
        }
    }
}

impl ToString for Value {
    /// Converts a `Value` to a string.
    ///
    /// # Returns
    ///
    /// A string representation of the value.
    fn to_string(&self) -> String {
        match self {
            Value::Column(Column {
                table_name,
                column_name,
            }) => format!("{}.{}", table_name, column_name),
            Value::Const(c) => c.to_string(),
        }
    }
}

impl Value {
    /// Extracts a constant value if the `Value` is a constant.
    ///
    /// # Returns
    ///
    /// An `Option` containing the constant value if it exists, otherwise `None`.
    fn get_const(&self) -> Option<Const> {
        match self {
            Value::Const(c) => Some(c.clone()),
            _ => None,
        }
    }

    /// Gets the table name from a `Value`.
    ///
    /// # Returns
    ///
    /// The table name as a string slice.
    fn get_table_name(&self) -> &str {
        match self {
            Value::Column(Column { table_name, .. }) => table_name,
            _ => panic!("Expected a column value"),
        }
    }

    /// Gets the column name from a `Value`.
    ///
    /// # Returns
    ///
    /// The column name as a string slice.
    fn get_column_name(&self) -> &str {
        match self {
            Value::Column(Column { column_name, .. }) => column_name,
            _ => panic!("Expected a column value"),
        }
    }

    /// Converts a serde JSON value to a `Value`.
    ///
    /// # Arguments
    ///
    /// * `value` - The serde JSON value.
    ///
    /// # Returns
    ///
    /// The corresponding `Value` object.
    fn from_serde_value(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Number(n) => Value::Const(Const::Number(n.as_i64().unwrap())),
            serde_json::Value::String(s) => Value::Const(Const::String(s.clone())),
            _ => panic!("Unexpected value type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{database, parser};

    // Define tests for View execution here
    #[test]
    fn test_execute_query() {
        let db_file_path = "database/movie_data.json";
        let db = database::load_database(db_file_path).unwrap();

        let query_file_path = "query";
        let query = std::fs::read_to_string(query_file_path).unwrap();
        let parsed_query = parser::parse_query(&query);

        let view = View::execute(parsed_query, db);

        assert_eq!(view.rows.len(), 2);
    }
}
