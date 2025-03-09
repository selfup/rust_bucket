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
mod tests {
    use super::*;

    const TEST: &str = "test";
    const COORDS: Coordinates = Coordinates { x: 42, y: 9000 };

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct Coordinates {
        pub x: i32,
        pub y: i32,
    }

    #[test]
    fn it_can_create_update_and_drop_a_table_and_take_any_struct_to_add_data() -> Result<()> {
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
    fn it_can_create_100_tables_and_drop_them_all() -> Result<()> {
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
    fn it_can_create_and_drop_an_empty_table() -> Result<()> {
        let table_name: &str = "empty";

        create_empty_table::<Coordinates>(&table_name)?;

        let contents: String = read_table(&table_name)?;
        let expected = "{\"table\":\"empty\",\"next_id\":\"0\",\"records\":{}}";

        assert_eq!(expected, contents);

        drop_table(&table_name)?;

        Ok(())
    }

    #[test]
    fn it_can_get_and_find() -> Result<()> {
        create_table("test3", &COORDS)?;

        assert_eq!(COORDS, find("test3", "0")?);

        drop_table("test3")?;

        Ok(())
    }

    #[test]
    fn it_can_return_json() -> Result<()> {
        create_table("test5", &COORDS)?;
        assert_eq!(COORDS, find("test5", "0")?);

        let b: String = read_table("test5")?;
        let c: String = json_table_records::<Coordinates>("test5")?;
        let d: String = json_find::<Coordinates>("test5", "0")?;

        let j = "{\"table\":\"test5\",\"next_id\":\"1\",\"records\":{\"0\":{\"x\":42,\"y\":9000}}}";
        assert_eq!(j, b);

        let k = "{\"0\":{\"x\":42,\"y\":9000}}";
        assert_eq!(k, c);

        let l = "{\"x\":42,\"y\":9000}";
        assert_eq!(l, d);

        drop_table("test5")?;

        Ok(())
    }

    #[test]
    fn it_can_delete_table_data_by_id() -> Result<()> {
        create_table("test6", &COORDS)?;

        assert_eq!(COORDS, find("test6", "0")?);

        let del = delete::<Coordinates>;
        del("test6", "0")?;

        let table = read_table("test6")?;
        assert_eq!(
            table,
            "{\"table\":\"test6\",\"next_id\":\"1\",\"records\":{\"0\":{}}}"
        );

        drop_table("test6")?;

        Ok(())
    }
}
