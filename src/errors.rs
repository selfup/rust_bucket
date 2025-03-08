// Copyright 2016 The Rust_Bucket Project Developers. See the COPYRIGHT file at
// the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option. This
// file may not be copied, modified, or distributed except according to those
// terms.

//! Error and Result module.

use std::error as std_error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::num::ParseIntError;
use std::result as std_result;

use serde_json;

// Bring the constructors of Error into scope so we can use them without an `Error::` incantation
use self::Error::{Io, NoSuchKey, NoSuchTable, ParseInt, Serde};

/// A Result alias often returned from methods that can fail for `rust_bucket` exclusive reasons.
pub type Result<T> = std_result::Result<T, Error>;

/// Errors that can occur during `rust_bucket` operations
#[derive(Debug)]
pub enum Error {
    /// Something went wrong internally while trying to perform IO.
    Io(io::Error),

    /// Problems with (de)serializing tables.
    ///
    /// `serde_json` makes no type-level distinction between serialization and deserialization
    /// errors, so we inherit that silliness.
    Serde(serde_json::Error),

    /// The stored `next_id` failed to parse.
    ///
    /// The `next_id` of a serialized table is an ASCII-encoded integer,
    /// which we failed to parse as an integer on reading it back in.
    /// This most likely indicates some kind of corruption.
    ParseInt(ParseIntError),

    /// The user tried to read a table, but no such table exists.
    NoSuchTable(String),

    /// The user tried to extract a key, but it didn't exist.
    NoSuchKey,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Io(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        Serde(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        ParseInt(err)
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> std_result::Result<(), fmt::Error> {
        match *self {
            Io(ref err) => {
                write!(formatter, "Error performing IO: ")?;
                err.fmt(formatter)
            }
            Serde(ref err) => {
                write!(formatter, "Error (de)serializing: ")?;
                err.fmt(formatter)
            }
            ParseInt(ref err) => {
                write!(formatter, "Error parsing an integer: ")?;
                err.fmt(formatter)
            }
            NoSuchTable(ref table) => {
                write!(
                    formatter,
                    "Tried to open the table \"{}\", which does not exist.",
                    table,
                )
            }
            NoSuchKey => write!(formatter, "Tried to retrieve a key which doesn't exist."),
        }
    }
}

impl std_error::Error for Error {
    // description is deprecated as of rust 1.42

    fn cause(&self) -> Option<&dyn std_error::Error> {
        match *self {
            Io(ref err) => Some(err),
            Serde(ref err) => Some(err),
            ParseInt(ref err) => Some(err),
            NoSuchTable(_) => None,
            NoSuchKey => None,
        }
    }
}
