use std::str::FromStr;

/// Represents a parsed SQL query.
#[derive(Debug)]
pub struct Query<'a> {
    pub select: Vec<Column>,
    pub from: String,
    pub joins: Vec<Join>,
    pub where_clause: Option<ValueTest>,
    input: Input<'a>,
}

/// Represents the input string being parsed.
#[derive(Debug)]
struct Input<'a> {
    src: &'a str,
    pos: usize,
}

/// Represents a column in a SQL query.
#[derive(Debug, Clone)]
pub struct Column {
    pub table_name: String,
    pub column_name: String,
}

/// Represents a JOIN clause in a SQL query.
#[derive(Debug)]
pub struct Join {
    pub table_name: String,
    pub on: ValueTest,
}

/// Represents a value test (e.g., a condition in a WHERE clause).
#[derive(Debug)]
pub struct ValueTest {
    pub left: Value,
    pub comparison: Comparison,
    pub right: Value,
}

/// Represents a value in a SQL query, which can be a column or a constant.
#[derive(Debug, Clone)]
pub enum Value {
    Column(Column),
    Const(Const),
}

/// Represents a constant value in a SQL query, which can be a number or a string.
#[derive(Debug, Clone)]
pub enum Const {
    Number(i64),
    String(String),
}

/// Represents a comparison operator in a SQL query.
#[derive(Debug, PartialEq)]
pub enum Comparison {
    Eq,
    Gt,
    Lt,
    Le,
    Ge,
    Ne,
}

impl FromStr for Comparison {
    type Err = ();

    /// Converts a string into a Comparison.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "=" => Ok(Comparison::Eq),
            ">" => Ok(Comparison::Gt),
            "<" => Ok(Comparison::Lt),
            "<=" => Ok(Comparison::Le),
            ">=" => Ok(Comparison::Ge),
            "<>" => Ok(Comparison::Ne),
            _ => Err(()),
        }
    }
}

impl<'a> Query<'a> {
    /// Creates a new Query instance from an input string.
    ///
    /// # Arguments
    ///
    /// * `input` - The SQL query string.
    fn new(input: &'a str) -> Self {
        Query {
            select: Vec::new(),
            from: String::new(),
            joins: Vec::new(),
            where_clause: None,
            input: Input::new(input),
        }
    }

    /// Parses the SQL query.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    fn parse(&mut self) -> Result<(), &'static str> {
        self.parse_select()?;
        self.parse_from()?;
        self.parse_joins()?;
        self.parse_where()?;
        Ok(())
    }

    /// Parses the SELECT clause of the SQL query.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    fn parse_select(&mut self) -> Result<(), &'static str> {
        self.input.consume_whitespace();
        self.input.expect("SELECT")?;
        self.input.consume_whitespace();

        loop {
            let table_name = self.input.consume_until(".")?.to_string();
            self.input.expect(".")?;
            let column_name = self
                .input
                .consume_until_any(&[',', ' ', '\n'])?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            self.select.push(Column {
                table_name,
                column_name,
            });
            self.input.consume_whitespace();
            if self.input.peek() == Some(',') {
                self.input.next();
                self.input.consume_whitespace();
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Parses the FROM clause of the SQL query.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    fn parse_from(&mut self) -> Result<(), &'static str> {
        self.input.consume_whitespace();
        self.input.expect("FROM")?;
        self.input.consume_whitespace();
        self.from = self
            .input
            .consume_until_any(&[' ', '\n'])?
            .trim_matches(&['\r', '\n'][..])
            .to_string();
        Ok(())
    }

    /// Parses the JOIN clauses of the SQL query.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    fn parse_joins(&mut self) -> Result<(), &'static str> {
        self.input.consume_whitespace();
        while self.input.peek() == Some('J') {
            self.input.expect("JOIN")?;
            self.input.consume_whitespace();

            let table_name = self
                .input
                .consume_until_any(&[' ', '\n'])?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            self.input.consume_whitespace();

            self.input.expect("ON")?;
            self.input.consume_whitespace();

            let left_table = self.input.consume_until(".")?.to_string();
            self.input.expect(".")?;

            let left_column = self
                .input
                .consume_until_any(&[' ', '\n'])?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            self.input.consume_whitespace();

            let comparison = self
                .input
                .consume_until_any(&[' ', '\n'])?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            self.input.consume_whitespace();

            let right_table = self.input.consume_until(".")?.to_string();
            self.input.expect(".")?;

            let right_column = self
                .input
                .consume_until_any(&[' ', '\n'])?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            self.input.consume_whitespace();

            self.joins.push(Join {
                table_name: table_name.to_string(),
                on: ValueTest {
                    left: Value::Column(Column {
                        table_name: left_table,
                        column_name: left_column.to_string(),
                    }),
                    comparison: Comparison::from_str(&comparison)
                        .map_err(|_| "Invalid comparison operator")?,
                    right: Value::Column(Column {
                        table_name: right_table,
                        column_name: right_column.to_string(),
                    }),
                },
            });
        }
        Ok(())
    }

    /// Parses the WHERE clause of the SQL query.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    fn parse_where(&mut self) -> Result<(), &'static str> {
        self.input.consume_whitespace();

        if self.input.peek() == Some('W') {
            self.input.expect("WHERE")?;
            self.input.consume_whitespace();

            // Parse the left value of the value-test
            let left = self.parse_value()?;
            self.input.consume_whitespace();

            // Parse the comparison operator
            let comparison = self
                .input
                .consume_until(" ")?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            self.input.consume_whitespace();

            // Parse the right value of the value-test
            let right = self.parse_value()?;

            // Set the where_clause with the parsed ValueTest
            self.where_clause = Some(ValueTest {
                left,
                comparison: Comparison::from_str(&comparison)
                    .map_err(|_| "Invalid comparison operator")?,
                right,
            });
        }
        Ok(())
    }

    /// Parses a value, which can be a column reference or a constant.
    ///
    /// # Returns
    ///
    /// A result containing the parsed value or an error message.
    fn parse_value(&mut self) -> Result<Value, &'static str> {
        self.input.consume_whitespace();

        if self.input.peek() == Some('\'') {
            // Parse single-quoted string constant
            let const_value = self.input.consume_until_any(&['\''])?.to_string();
            self.input.expect("'")?;
            Ok(Value::Const(Const::String(const_value)))
        } else if self.input.peek().map_or(false, |c| c.is_digit(10)) {
            // Parse numeric constant
            let const_value = self
                .input
                .consume_until_any(&[' ', '\n', '\r'])?
                .trim_matches(&['\r', '\n'][..])
                .parse::<i64>()
                .map_err(|_| "Failed to parse number")?;
            Ok(Value::Const(Const::Number(const_value)))
        } else {
            // Parse column-id
            let table_name = self
                .input
                .consume_until(".")?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            self.input.expect(".")?;
            let column_name = self
                .input
                .consume_until_any(&[' ', '\n', '\r'])?
                .trim_matches(&['\r', '\n'][..])
                .to_string();
            Ok(Value::Column(Column {
                table_name,
                column_name,
            }))
        }
    }
}

impl<'a> Input<'a> {
    /// Creates a new Input instance from a source string.
    ///
    /// # Arguments
    ///
    /// * `src` - The source string.
    fn new(src: &'a str) -> Self {
        Input { src, pos: 0 }
    }

    /// Consumes whitespace characters from the input.
    fn consume_whitespace(&mut self) {
        while self
            .peek()
            .map_or(false, |c| c.is_whitespace() || c == '\r' || c == '\n')
        {
            self.next();
        }
    }

    /// Returns the next character from the input, advancing the position.
    fn next(&mut self) -> Option<char> {
        if self.pos < self.src.len() {
            let ch = self.src[self.pos..].chars().next().unwrap();
            self.pos += ch.len_utf8();
            Some(ch)
        } else {
            None
        }
    }

    /// Peeks at the next character without advancing the position.
    fn peek(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    /// Consumes characters until the specified character is encountered.
    ///
    /// # Arguments
    ///
    /// * `until` - The character to consume until.
    ///
    /// # Returns
    ///
    /// A result containing the consumed string or an error message.
    fn consume_until(&mut self, until: &str) -> Result<&'a str, &'static str> {
        let start = self.pos;
        while self.peek().map_or(false, |c| !until.contains(c)) {
            self.next();
        }
        Ok(&self.src[start..self.pos])
    }

    /// Consumes characters until any of the specified characters are encountered.
    ///
    /// # Arguments
    ///
    /// * `until` - A slice of characters to consume until.
    ///
    /// # Returns
    ///
    /// A result containing the consumed string or an error message.
    fn consume_until_any(&mut self, until: &[char]) -> Result<&'a str, &'static str> {
        let start = self.pos;
        while self.peek().map_or(false, |c| !until.contains(&c)) {
            self.next();
        }
        Ok(&self.src[start..self.pos])
    }

    /// Expects the next characters to match the specified string.
    ///
    /// # Arguments
    ///
    /// * `expected` - The expected string.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    fn expect(&mut self, expected: &str) -> Result<(), &'static str> {
        for expected_char in expected.chars() {
            if self.next() != Some(expected_char) {
                return Err("Unexpected character");
            }
        }
        Ok(())
    }
}

/// Parses an SQL query string into a Query instance.
///
/// # Arguments
///
/// * `input` - The SQL query string.
///
/// # Returns
///
/// A parsed Query instance.
pub fn parse_query(input: &str) -> Query {
    let mut parsed_query = Query::new(input);
    parsed_query.parse().unwrap();
    parsed_query
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests parsing of the SELECT clause.
    #[test]
    fn test_parse_select() {
        let mut query = Query::new("SELECT table1.col1, table2.col2 FROM table1");
        query.parse_select().unwrap();
        assert_eq!(query.select.len(), 2);
        assert_eq!(query.select[0].table_name, "table1");
        assert_eq!(query.select[0].column_name, "col1");
        assert_eq!(query.select[1].table_name, "table2");
        assert_eq!(query.select[1].column_name, "col2");
    }

    /// Tests parsing of the FROM clause.
    #[test]
    fn test_parse_from() {
        let mut query = Query::new("FROM table1");
        query.parse_from().unwrap();
        assert_eq!(query.from, "table1");
    }

    /// Tests parsing of the WHERE clause.
    #[test]
    fn test_parse_where() {
        let mut query = Query::new("WHERE table1.col1 = 42");
        query.parse_where().unwrap();
        let where_clause = query.where_clause.unwrap();
        match where_clause.left {
            Value::Column(Column {
                table_name,
                column_name,
            }) => {
                assert_eq!(table_name, "table1");
                assert_eq!(column_name, "col1");
            }
            _ => panic!("Expected column value"),
        }
        assert_eq!(where_clause.comparison, Comparison::Eq);
        match where_clause.right {
            Value::Const(Const::Number(n)) => assert_eq!(n, 42),
            _ => panic!("Expected number constant"),
        }
    }
}
