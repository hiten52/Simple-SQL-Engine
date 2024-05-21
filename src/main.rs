mod database;

fn main() {
    let database_file_name = "database/movie_data.json";
    let db = database::load_database(database_file_name).unwrap();

    println!("{db:?}");
}