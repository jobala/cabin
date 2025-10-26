use tempfile::tempdir;

pub fn get_temp_dir() -> String {
    String::from(tempdir().unwrap().path().to_str().unwrap())
}
