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
use std::io::prelude::*;
use std::path::Path;

pub mod errors;
use errors::{Error, Result};

// Structure for data storage *********************************************************************

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableData<T: Serialize> {
    pub table: String,
    pub next_id: String,
    pub records: HashMap<String, T>,
}

// Public functions *******************************************************************************

pub fn update_table<T: Serialize>(table: &str, t: &T) -> Result<()> {
    let serialized = serde_json::to_string(&create_base_data(table, t))?;
    let db_table = Path::new("./db").join(table);

    let mut buffer = File::create(db_table)?;
    buffer.write_all(serialized.as_bytes())?;

    Ok(())
}

pub fn create_table<T: Serialize>(table: &str, t: &T) -> Result<()> {
    create_db_dir()?;

    let serialized = serde_json::to_string(&create_base_data(table, t))?;
    let db_table = Path::new("./db").join(table);

    if db_table.exists() {
        return Ok(());
    }

    let mut buffer = File::create(db_table)?;
    buffer.write_all(serialized.as_bytes())?;

    Ok(())
}

pub fn create_empty_table<T: Serialize>(table: &str) -> Result<()> {
    create_db_dir()?;

    let record: HashMap<String, T> = HashMap::new();

    let data = TableData {
        table: table.to_string(),
        next_id: "0".to_string(),
        records: record,
    };

    let serialized = serde_json::to_string(&data)?;
    let db_table = Path::new("./db").join(table);

    if db_table.exists() {
        return Ok(());
    }

    let mut buffer = File::create(db_table)?;
    buffer.write_all(serialized.as_bytes())?;

    Ok(())
}

pub fn read_table(table: &str) -> Result<String> {
    let db_table = Path::new("./db").join(table.to_owned());

    let mut file = match File::open(db_table) {
        Ok(file) => file,
        Err(ref err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(Error::NoSuchTable(table.to_owned()))
        }
        Err(err) => return Err(Error::Io(err)),
    };

    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;

    Ok(buffer)
}

pub fn drop_table(table: &str) -> io::Result<()> {
    let table_path = Path::new("./db").join(table);
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

    let db_table = Path::new("./db").join(table);

    if db_table.exists() {
        return Ok(());
    }

    let mut buffer = File::create(db_table)?;
    buffer.write_all(json.as_bytes())?;

    Ok(())
}

pub fn update_json(table: &str, json: &str) -> Result<()> {
    create_db_dir()?;

    let db_table = Path::new("./db").join(table);

    let mut buffer = File::create(db_table)?;
    buffer.write_all(json.as_bytes())?;

    Ok(())
}

// Private functions ******************************************************************************

fn upgrade_table<T: Serialize>(table: &str, t: &T) -> Result<()> {
    let serialized = serde_json::to_string(t)?;
    let db_table = Path::new("./db").join(table);

    let mut buffer = File::create(db_table)?;
    buffer.write_all(serialized.as_bytes())?;

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

fn create_db_dir() -> io::Result<()> {
    if Path::new("./db").exists() {
        return Ok(());
    }

    fs::create_dir("db")
}

// Tests ******************************************************************************************

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct Coordinates {
        pub x: i32,
        pub y: i32,
    }

    #[test]
    fn it_can_create_update_and_drop_a_table_and_take_any_struct_to_add_data() {
        let a = Coordinates { x: 42, y: 9000 };
        let b = Coordinates { x: 32, y: 8765 };
        let c = Coordinates { x: 23, y: 900 };
        let d = Coordinates { x: 105, y: 7382 };

        let e = "{\"table\":\"test\",\"next_id\":\"1\",\"records\":{\"0\":{\"x\":42,\"y\":9000}}}";
        let f = "{\"table\":\"test\",\"next_id\":\"1\",\"records\":{\"0\":{\"x\":32,\"y\":8765}}}";

        create_table("test", &a).unwrap();
        assert_eq!(e, read_table("test").unwrap());

        update_table("test", &b).unwrap();
        assert_eq!(f, read_table("test").unwrap());

        drop_table("test").unwrap();
        create_table("test", &a).unwrap();

        append_records("test", b).unwrap();
        append_records("test", c).unwrap();
        append_records("test", d).unwrap();

        assert!(read_table("test").unwrap().contains("2"));
        assert!(read_table("test").unwrap().contains("3"));
        assert!(read_table("test").unwrap().contains("4"));

        drop_table("test").unwrap();
    }

    #[test]
    fn it_can_create_100_tables_and_drop_them_all() {
        for n in 1..101 {
            let table = format!("{}", n);
            let a = Coordinates { x: 42, y: 9000 };

            create_table(&*table, &a).unwrap();
        }

        for k in 1..101 {
            let table = format!("{}", k);

            drop_table(&*table).unwrap();
        }
    }

    #[test]
    fn it_can_create_and_drop_an_empty_table() {
        let table_name = format!("empty");

        create_empty_table::<Coordinates>(&*table_name).unwrap();

        let contents: String = read_table(&*table_name).unwrap();
        let expected = "{\"table\":\"empty\",\"next_id\":\"0\",\"records\":{}}";

        assert_eq!(expected, contents);

        drop_table(&*table_name).unwrap();
    }

    #[test]
    fn it_can_get_and_find() {
        let a = Coordinates { x: 42, y: 9000 };

        create_table("test3", &a).unwrap();

        assert_eq!(a, find("test3", "0").unwrap());

        drop_table("test3").unwrap();
    }

    #[test]
    fn it_can_return_json() {
        let a = Coordinates { x: 42, y: 9000 };
        create_table("test5", &a).unwrap();
        assert_eq!(a, find("test5", "0").unwrap());

        let b: String = read_table("test5").unwrap();
        let c: String = json_table_records::<Coordinates>("test5").unwrap();
        let d: String = json_find::<Coordinates>("test5", "0").unwrap();

        let j = "{\"table\":\"test5\",\"next_id\":\"1\",\"records\":{\"0\":{\"x\":42,\"y\":9000}}}";
        assert_eq!(j, b);

        let k = "{\"0\":{\"x\":42,\"y\":9000}}";
        assert_eq!(k, c);

        let l = "{\"x\":42,\"y\":9000}";
        assert_eq!(l, d);

        drop_table("test5").unwrap();
    }

    #[test]
    fn it_can_delete_table_data_by_id() {
        let a = Coordinates { x: 42, y: 9000 };

        create_table("test6", &a).unwrap();

        assert_eq!(a, find("test6", "0").unwrap());

        let del = delete::<Coordinates>;
        del("test6", "0").unwrap();

        let table = read_table("test6").unwrap();
        assert_eq!(
            table,
            "{\"table\":\"test6\",\"next_id\":\"1\",\"records\":{\"0\":{}}}"
        );

        drop_table("test6").unwrap();
    }
}
