// Copyright 2016 The Rust_Bucket Project Developers. See the COPYRIGHT file at
// the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option. This
// file may not be copied, modified, or distributed except according to those
// terms.

extern crate serde;
extern crate serde_json;

use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;

pub mod errors;
use errors::{Error, Result};

const DB_PATH: &str = "./db";

// Structure for data storage *********************************************************************

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableData<T: Serialize> {
    pub table: String,
    pub next_id: String,
    pub records: HashMap<String, T>,
}

// Public functions *******************************************************************************

pub fn update_table<T: Serialize>(table: &str, t: &T) -> Result<()> {
    let db_table = db_table(table);

    let writer = buffed_writer(db_table)?;

    let data = &create_base_data(table, t);

    serde_json::to_writer(writer, data)?;

    Ok(())
}

pub fn create_table<T: Serialize>(table: &str, t: &T) -> Result<()> {
    create_db_dir()?;

    let db_table = db_table(table);

    if db_table.exists() {
        return Ok(());
    }

    let writer = buffed_writer(db_table)?;

    let data = &create_base_data(table, t);

    serde_json::to_writer(writer, data)?;

    Ok(())
}

pub fn create_empty_table<T: Serialize>(table: &str) -> Result<()> {
    create_db_dir()?;

    let db_table = db_table(table);

    if db_table.exists() {
        return Ok(());
    }

    let file = File::create(db_table)?;

    let record: HashMap<String, T> = HashMap::new();

    let data = TableData {
        table: table.to_string(),
        next_id: "0".to_string(),
        records: record,
    };

    serde_json::to_writer(file, &data)?;

    Ok(())
}

pub fn read_table(table: &str) -> Result<String> {
    let db_table = Path::new(DB_PATH).join(table.to_owned());

    let mut file = match File::open(db_table) {
        Ok(file) => file,
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(Error::NoSuchTable(table.to_owned()));
        }
        Err(err) => return Err(Error::Io(err)),
    };

    let mut buffer = String::new();

    file.read_to_string(&mut buffer)?;

    Ok(buffer)
}

pub fn drop_table(table: &str) -> io::Result<()> {
    let table_path = Path::new(DB_PATH).join(table);

    fs::remove_file(table_path)?;

    Ok(())
}

pub fn append_records<T>(table: &str, t: T) -> Result<()>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let mut data = get_table(table)?;

    let increased_next_id = data.next_id.parse::<i32>()?;

    let new_id = increased_next_id + 1;

    data.records.insert(increased_next_id.to_string(), t);

    data.next_id = new_id.to_string();

    upgrade_table(table, &data)
}

pub fn get_table<T>(table: &str) -> Result<TableData<T>>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let result = read_table(table)?;

    serde_json::from_str(&result).map_err(Error::from)
}

pub fn get_table_records<T>(table: &str) -> Result<HashMap<String, T>>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    Ok(get_table(table)?.records)
}

pub fn find<T>(table: &str, id: &str) -> Result<T>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    get_table_records(table)?.remove(id).ok_or(Error::NoSuchKey)
}

pub fn delete<T>(table: &str, id: &str) -> Result<()>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let mut current_table: HashMap<String, T> = get_table_records(table)?;

    current_table.remove(id);

    update_table(table, &current_table)
}

pub fn json_find<T>(table: &str, id: &str) -> Result<String>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let incoming_record: T = find(table, id)?;

    serde_json::to_string(&incoming_record).map_err(Error::from)
}

pub fn json_table_records<T>(table: &str) -> Result<String>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let records: HashMap<String, T> = get_table_records(table)?;

    serde_json::to_string(&records).map_err(Error::from)
}

pub fn store_json(table: &str, json: &str) -> Result<()> {
    create_db_dir()?;

    let db_table = db_table(table);

    if db_table.exists() {
        return Ok(());
    }

    let writer = buffed_writer(db_table)?;

    serde_json::to_writer(writer, json)?;

    Ok(())
}

pub fn update_json(table: &str, json: &str) -> Result<()> {
    create_db_dir()?;

    let db_table = db_table(table);

    let writer = buffed_writer(db_table)?;

    serde_json::to_writer(writer, json)?;

    Ok(())
}

pub fn count_records<T>(table: &str) -> Result<usize>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let records = get_table_records::<T>(table)?;

    Ok(records.len())
}

pub fn batch_insert<T>(table: &str, records: Vec<T>) -> Result<()>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let mut data = get_table(table)?;

    let mut next_id = data.next_id.parse::<i32>()?;

    for record in records {
        data.records.insert(next_id.to_string(), record);

        next_id += 1;
    }

    data.next_id = next_id.to_string();

    upgrade_table(table, &data)
}

pub fn update_record<T>(table: &str, id: &str, record: T) -> Result<()>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let mut data = get_table(table)?;

    if !data.records.contains_key(id) {
        return Err(Error::NoSuchKey);
    }

    data.records.insert(id.to_string(), record);

    upgrade_table(table, &data)
}

pub fn table_exists(table: &str) -> bool {
    db_table(table).exists()
}

pub fn list_tables() -> io::Result<Vec<String>> {
    let mut tables = Vec::new();

    if !Path::new(DB_PATH).exists() {
        return Ok(tables);
    }

    for entry in fs::read_dir(DB_PATH)? {
        let entry = entry?;

        if let Some(name) = entry.file_name().to_str() {
            tables.push(name.to_string());
        }
    }

    Ok(tables)
}

pub fn find_by<T, F>(table: &str, predicate: F) -> Result<HashMap<String, T>>
where
    T: for<'a> Deserialize<'a> + Serialize,
    F: Fn(&T) -> bool,
{
    let all_records = get_table_records::<T>(table)?;

    let mut matching_records = HashMap::new();

    for (id, record) in all_records {
        if predicate(&record) {
            matching_records.insert(id, record);
        }
    }

    Ok(matching_records)
}

pub fn clear_table<T>(table: &str) -> Result<()>
where
    T: for<'a> Deserialize<'a> + Serialize,
{
    let mut data = get_table::<T>(table)?;

    data.records.clear();

    data.next_id = "0".to_string();

    upgrade_table(table, &data)
}

// Private functions ******************************************************************************

fn db_table(table: &str) -> std::path::PathBuf {
    Path::new(DB_PATH).join(table)
}

fn buffed_writer(db_table: PathBuf) -> Result<BufWriter<File>> {
    let file = File::create(db_table)?;

    let writer = BufWriter::new(file);

    Ok(writer)
}

fn upgrade_table<T: Serialize>(table: &str, t: &T) -> Result<()> {
    let db_table = db_table(table);

    let file = File::create(db_table)?;

    let writer = BufWriter::new(file);

    serde_json::to_writer(writer, t)?;

    Ok(())
}

fn create_base_data<T: Serialize>(table: &str, t: T) -> TableData<T> {
    let mut record = HashMap::new();

    record.insert("0".to_string(), t);

    TableData {
        table: table.to_string(),
        next_id: "1".to_string(),
        records: record,
    }
}

fn create_db_dir() -> Result<()> {
    if Path::new(DB_PATH).exists() {
        return Ok(());
    }

    fs::create_dir("db")?;

    Ok(())
}

// Tests ******************************************************************************************

#[cfg(test)]
mod the_db {
    use super::*;

    const TEST: &str = "test";
    const COORDS: Coordinates = Coordinates { x: 42, y: 9000 };

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct Coordinates {
        pub x: i32,
        pub y: i32,
    }

    #[test]
    fn can_crud_generic_data() -> Result<()> {
        let b = Coordinates { x: 32, y: 8765 };
        let c = Coordinates { x: 23, y: 900 };
        let d = Coordinates { x: 105, y: 7382 };

        let e = "{\"table\":\"test\",\"next_id\":\"1\",\"records\":{\"0\":{\"x\":42,\"y\":9000}}}";
        let f = "{\"table\":\"test\",\"next_id\":\"1\",\"records\":{\"0\":{\"x\":32,\"y\":8765}}}";

        create_table(TEST, &COORDS)?;
        assert_eq!(e, read_table(TEST)?);

        update_table(TEST, &b)?;
        assert_eq!(f, read_table(TEST)?);

        drop_table(TEST)?;
        create_table(TEST, &COORDS)?;

        append_records(TEST, b)?;
        append_records(TEST, c)?;
        append_records(TEST, d)?;

        let result = read_table(TEST)?;

        assert!(result.contains("2"));
        assert!(result.contains("3"));
        assert!(result.contains("4"));

        drop_table(TEST)?;

        Ok(())
    }

    #[test]
    fn can_create_100_tables_and_drop_them_all() -> Result<()> {
        for n in 1..101 {
            let table = format!("{}", n);

            create_table(&*table, &COORDS)?;
        }

        for k in 1..101 {
            let table = format!("{}", k);

            drop_table(&*table)?;
        }

        Ok(())
    }

    #[test]
    fn can_create_and_drop_an_empty_table() -> Result<()> {
        let table_name: &str = "empty";

        create_empty_table::<Coordinates>(&table_name)?;

        let contents: String = read_table(&table_name)?;
        let expected = "{\"table\":\"empty\",\"next_id\":\"0\",\"records\":{}}";

        assert_eq!(expected, contents);

        drop_table(&table_name)?;

        Ok(())
    }

    #[test]
    fn can_get_and_find() -> Result<()> {
        create_table("test_3", &COORDS)?;

        assert_eq!(COORDS, find("test_3", "0")?);

        drop_table("test_3")?;

        Ok(())
    }

    #[test]
    fn can_return_json() -> Result<()> {
        create_table("test_5", &COORDS)?;
        assert_eq!(COORDS, find("test_5", "0")?);

        let b: String = read_table("test_5")?;
        let c: String = json_table_records::<Coordinates>("test_5")?;
        let d: String = json_find::<Coordinates>("test_5", "0")?;

        let j =
            "{\"table\":\"test_5\",\"next_id\":\"1\",\"records\":{\"0\":{\"x\":42,\"y\":9000}}}";

        assert_eq!(j, b);

        let k = "{\"0\":{\"x\":42,\"y\":9000}}";
        assert_eq!(k, c);

        let l = "{\"x\":42,\"y\":9000}";
        assert_eq!(l, d);

        drop_table("test_5")?;

        Ok(())
    }

    #[test]
    fn can_delete_table_data_by_id() -> Result<()> {
        create_table("test_6", &COORDS)?;

        assert_eq!(COORDS, find("test_6", "0")?);

        let del = delete::<Coordinates>;

        del("test_6", "0")?;

        let table = read_table("test_6")?;
        assert_eq!(
            table,
            "{\"table\":\"test_6\",\"next_id\":\"1\",\"records\":{\"0\":{}}}"
        );

        drop_table("test_6")?;

        Ok(())
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct TestRecord {
        pub name: String,
        pub value: i32,
    }

    #[test]
    fn can_test_non_existent_table_read() {
        let result = read_table("non_existent_table");

        assert!(matches!(result, Err(Error::NoSuchTable(_))));
    }

    #[test]
    fn can_test_empty_id_find() -> Result<()> {
        let test_table = "empty_id_test";

        let test_record = TestRecord {
            name: "test".to_string(),
            value: 42,
        };

        let second_test_record = TestRecord {
            name: "empty_id".to_string(),
            value: 100,
        };

        create_table(test_table, &test_record)?;

        let mut table_data = get_table::<TestRecord>(test_table)?;

        let empty_string = "".to_string();

        table_data.records.insert(empty_string, second_test_record);

        let db_path = Path::new("./db").join(test_table);

        let file = std::fs::File::create(db_path)?;

        serde_json::to_writer(file, &table_data)?;

        let found = find::<TestRecord>(test_table, "")?;

        assert_eq!(found.name, "empty_id");

        drop_table(test_table)?;

        Ok(())
    }

    #[test]
    fn can_test_special_chars_in_table_name() -> Result<()> {
        let test_table = "special!@#$%^&*()_+";

        let test_record = TestRecord {
            name: "test".to_string(),
            value: 42,
        };

        create_table(test_table, &test_record)?;

        let read_record: TestRecord = find(test_table, "0")?;

        assert_eq!(read_record, test_record);

        drop_table(test_table)?;

        Ok(())
    }

    #[test]
    fn can_test_concurrent_modifications() -> Result<()> {
        let test_table = "concurrent_test";

        let test_record = TestRecord {
            name: "original".to_string(),
            value: 42,
        };

        let second_test_record = TestRecord {
            name: "first".to_string(),
            value: 100,
        };

        create_table(test_table, &test_record)?;

        append_records(test_table, second_test_record)?;

        let modified_record = TestRecord {
            name: "modified".to_string(),
            value: 200,
        };

        append_records(test_table, modified_record)?;

        let records = get_table_records::<TestRecord>(test_table)?;

        assert_eq!(records.len(), 3); // Original + 2 appends

        drop_table(test_table)?;

        Ok(())
    }

    #[test]
    fn can_test_count_records() -> Result<()> {
        let table_name = "count_test";

        create_table(table_name, &COORDS)?;

        assert_eq!(count_records::<Coordinates>(table_name)?, 1);

        append_records(table_name, Coordinates { x: 10, y: 20 })?;

        append_records(table_name, Coordinates { x: 30, y: 40 })?;

        assert_eq!(count_records::<Coordinates>(table_name)?, 3);

        drop_table(table_name)?;

        Ok(())
    }

    #[test]
    fn can_test_batch_insert() -> Result<()> {
        let table_name = "batch_test";

        create_empty_table::<Coordinates>(table_name)?;

        assert_eq!(count_records::<Coordinates>(table_name)?, 0);

        let batch = vec![
            Coordinates { x: 1, y: 2 },
            Coordinates { x: 3, y: 4 },
            Coordinates { x: 5, y: 6 },
        ];

        batch_insert(table_name, batch)?;

        assert_eq!(count_records::<Coordinates>(table_name)?, 3);

        let records = get_table_records::<Coordinates>(table_name)?;

        assert!(records.contains_key("0"));

        assert!(records.contains_key("1"));

        assert!(records.contains_key("2"));

        drop_table(table_name)?;

        Ok(())
    }

    #[test]
    fn can_test_update_record() -> Result<()> {
        let table_name = "update_test";

        create_table(table_name, &COORDS)?;

        let record: Coordinates = find(table_name, "0")?;

        assert_eq!(record, COORDS);

        let updated = Coordinates { x: 999, y: 888 };

        let expected = Coordinates { x: 999, y: 888 };

        update_record(table_name, "0", updated)?;

        let record: Coordinates = find(table_name, "0")?;

        assert_eq!(record, expected);

        let result = update_record::<Coordinates>(table_name, "999", COORDS);

        assert!(matches!(result, Err(Error::NoSuchKey)));

        drop_table(table_name)?;
        Ok(())
    }

    #[test]
    fn can_test_table_exists() -> Result<()> {
        let table_name = "exists_test";

        assert!(!table_exists(table_name));

        create_table(table_name, &COORDS)?;

        assert!(table_exists(table_name));

        drop_table(table_name)?;

        assert!(!table_exists(table_name));

        Ok(())
    }

    #[test]
    fn can_test_list_tables() -> Result<()> {
        let table_names = ["test_1", "test_2", "test_3"];

        for &name in &table_names {
            if table_exists(name) {
                drop_table(name)?;
            }
        }

        for &name in &table_names {
            create_table(name, &COORDS)?;
        }

        let tables = list_tables()?;

        for &name in &table_names {
            assert!(tables.contains(&name.to_string()));
        }

        for &name in &table_names {
            drop_table(name)?;
        }

        Ok(())
    }

    #[test]
    fn can_test_find_by() -> Result<()> {
        let table_name = "find_by_test";

        create_empty_table::<Coordinates>(table_name)?;

        let test_data = vec![
            Coordinates { x: 10, y: 10 },
            Coordinates { x: 20, y: 10 },
            Coordinates { x: 30, y: 30 },
            Coordinates { x: 40, y: 10 },
        ];

        batch_insert(table_name, test_data)?;

        let matching = find_by::<Coordinates, _>(table_name, |coord| coord.y == 10)?;

        assert_eq!(matching.len(), 3);

        let matching = find_by::<Coordinates, _>(table_name, |coord| coord.x > 20)?;

        assert_eq!(matching.len(), 2);

        drop_table(table_name)?;

        Ok(())
    }

    #[test]
    fn can_test_clear_table() -> Result<()> {
        let table_name = "clear_test";

        create_table(table_name, &COORDS)?;

        append_records(table_name, Coordinates { x: 100, y: 200 })?;

        append_records(table_name, Coordinates { x: 300, y: 400 })?;

        assert_eq!(count_records::<Coordinates>(table_name)?, 3);

        clear_table::<Coordinates>(table_name)?;

        assert_eq!(count_records::<Coordinates>(table_name)?, 0);

        let table_data = get_table::<Coordinates>(table_name)?;

        assert_eq!(table_data.next_id, "0");

        append_records(table_name, COORDS)?;

        assert_eq!(count_records::<Coordinates>(table_name)?, 1);

        drop_table(table_name)?;

        Ok(())
    }
}
