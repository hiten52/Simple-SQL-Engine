use std::fs;

mod database;
mod parser;

fn main() {
    let database_file_path = "database/movie_data.json";
    let db = database::load_database(database_file_path).unwrap();
    
    let query_file_path = "query";
    let sql_query = fs::read_to_string(query_file_path).unwrap();
   
    println!("{db:?}\n");
    println!("{sql_query}\n");
    println!("{:?}\n", parser::parse_query(&sql_query));
}