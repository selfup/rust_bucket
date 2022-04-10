use rust_bucket::*;

use criterion::{criterion_group, criterion_main, Criterion};

extern crate serde;
extern crate serde_json;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Coordinates {
    pub x: i32,
    pub y: i32,
}

fn bench_create_table(crit: &mut Criterion) {
    let object = Coordinates { x: 42, y: 9000 };

    crit.bench_function("create table", |b| {
        b.iter(|| create_table("test4", &object).unwrap())
    });
}

fn bench_update_table(crit: &mut Criterion) {
    let object = Coordinates { x: 42, y: 9000 };

    crit.bench_function("update table", |b| {
        b.iter(|| update_table("test2", &object).unwrap())
    });
}

fn bench_read_table(crit: &mut Criterion) {
    crit.bench_function("read_table", |b| b.iter(|| read_table("test2").unwrap()));
}

fn bench_json_table_records(crit: &mut Criterion) {
    let a = json_table_records::<Coordinates>;

    crit.bench_function("json_table_records", |b| b.iter(|| a("test2").unwrap()));
}

fn bench_json_find(crit: &mut Criterion) {
    let a = json_find::<Coordinates>;

    crit.bench_function("json_find", |b| b.iter(|| a("test2", "0").unwrap()));
}

fn bench_find(crit: &mut Criterion) {
    let a = find::<Coordinates>;
    crit.bench_function("find", |b| b.iter(|| a("test2", "0").unwrap()));
}

fn bench_store_update_read_and_delete_json(crit: &mut Criterion) {
    crit.bench_function("store_json", |b| {
        b.iter(|| store_json("test7", "{\"x\":42,\"y\":9000}}}").unwrap())
    });

    update_json("test7", "{\"x\":45,\"y\":9876}}}").unwrap();
    read_table("test7").unwrap();
    drop_table("test7").unwrap();
}

criterion_group!(
    benches,
    bench_create_table,
    bench_find,
    bench_json_find,
    bench_json_table_records,
    bench_read_table,
    bench_store_update_read_and_delete_json,
    bench_update_table
);

criterion_main!(benches);
