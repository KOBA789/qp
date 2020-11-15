use serde::{Deserialize, Serialize};

use crate::btree;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    GetItem(GetItemInput),
    PutItem(PutItemInput),
    DeleteItem(DeleteItemInput),
    CreateTable(CreateTableInput),
    ScanItem(ScanItemInput),
    Flush(FlushInput),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Key([u8; 8]);
impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        hex::serialize_upper(self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Key(hex::deserialize(deserializer)?))
    }
}
impl From<Key> for btree::Key {
    fn from(Key(bytes): Key) -> Self {
        bytes
    }
}
impl From<btree::Key> for Key {
    fn from(bytes: btree::Key) -> Self {
        Key(bytes)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Item {
    pub key: Key,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct GetItemInput {
    pub table_id: Key,
    pub key: Key,
}

#[derive(Debug, Deserialize)]
pub struct PutItemInput {
    pub table_id: Key,
    pub item: Item,
}

#[derive(Debug, Deserialize)]
pub struct DeleteItemInput {
    pub table_id: Key,
    pub key: Key,
}

#[derive(Debug, Deserialize)]
pub struct ScanItemInput {
    pub table_id: Key,
    pub start: Option<Key>,
    pub backward: bool,
    pub limit: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableInput {
    pub table_id: Key,
}

#[derive(Debug, Deserialize)]
pub struct FlushInput;

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum Response {
    GetItem(GetItemOutput),
    PutItem(PutItemOutput),
    DeleteItem(DeleteItemOutput),
    ScanItem(ScanItemOutput),
    CreateTable(CreateTableOutput),
    Flush(FlushOutput),
    Error(Error),
}

#[derive(Debug, Serialize)]
pub struct GetItemOutput {
    pub item: Option<Item>,
}

#[derive(Debug, Serialize)]
pub struct PutItemOutput;

#[derive(Debug, Serialize)]
pub struct DeleteItemOutput {
    pub found: bool,
}

#[derive(Debug, Serialize)]
pub struct ScanItemOutput {
    pub items: Vec<Item>,
}

#[derive(Debug, Serialize)]
pub struct CreateTableOutput;

#[derive(Debug, Serialize)]
pub struct FlushOutput;

#[derive(Debug, Serialize)]
#[serde(tag = "error")]
pub enum Error {
    Deadlock,
    Other { message: String },
}
