use std::fs;

mod database;

fn main() {
    let database_file_path = "database/movie_data.json";
    let db = database::load_database(database_file_path).unwrap();
    
    let query_file_path = "query";
    let sql_query = fs::read_to_string(query_file_path).unwrap();
   
    println!("{db:?}");
    println!("{sql_query}");
}