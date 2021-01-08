use std::{convert::TryInto, sync::Arc};

use crate::{
    btree,
    buffer::BufferPoolManager,
    disk::PageId,
    query::CreateTableOutput,
    query::FlushInput,
    query::{
        self, CreateTableInput, DeleteItemInput, DeleteItemOutput, FlushOutput, GetItemInput,
        GetItemOutput, PutItemInput, PutItemOutput, Request, Response, ScanItemInput,
        ScanItemOutput,
    },
};

pub struct Executor {
    bufmgr: Arc<BufferPoolManager>,
}

impl Executor {
    pub fn new(bufmgr: Arc<BufferPoolManager>) -> Self {
        Self { bufmgr }
    }

    pub fn execute(&self, request: Request) -> query::Response {
        let resp = match request {
            Request::GetItem(input) => self.get_item(input).map(Response::GetItem),
            Request::PutItem(input) => self.put_item(input).map(Response::PutItem),
            Request::DeleteItem(input) => self.delete_item(input).map(Response::DeleteItem),
            Request::CreateTable(input) => self.create_table(input).map(Response::CreateTable),
            Request::ScanItem(input) => self.scan_item(input).map(Response::ScanItem),
            Request::Flush(input) => self.flush(input).map(Response::Flush),
        };
        resp.map_err(|err| match err.downcast_ref::<btree::Error>() {
            Some(btree::Error::Deadlock) => query::Error::Deadlock,
            _ => query::Error::Other {
                message: err.to_string(),
            },
        })
        .unwrap_or_else(Response::Error)
    }

    fn lookup_table(&self, table_id: btree::Key) -> Result<PageId, anyhow::Error> {
        let catalog = btree::Access::open(&self.bufmgr, PageId::CATALOG_PAGE_ID);
        let mut buf = vec![];
        if !catalog.get(table_id, &mut buf)? {
            return Err(anyhow::anyhow!("no such table"));
        }
        Ok(buf[..].try_into()?)
    }

    fn get_item(&self, input: GetItemInput) -> Result<GetItemOutput, anyhow::Error> {
        let page_id = self.lookup_table(input.table_id.into())?;
        let table_access = btree::Access::open(&self.bufmgr, page_id);
        let mut buf = vec![];
        if !table_access.get(input.key.into(), &mut buf)? {
            return Ok(GetItemOutput { item: None });
        }
        let item = query::Item {
            key: input.key,
            value: String::from_utf8(buf)?,
        };
        Ok(GetItemOutput { item: Some(item) })
    }

    fn put_item(&self, input: PutItemInput) -> Result<PutItemOutput, anyhow::Error> {
        let page_id = self.lookup_table(input.table_id.into())?;
        let table_access = btree::Access::open(&self.bufmgr, page_id);
        table_access.put(input.item.key.into(), input.item.value.as_bytes())?;
        Ok(PutItemOutput)
    }

    fn delete_item(&self, _input: DeleteItemInput) -> Result<DeleteItemOutput, anyhow::Error> {
        todo!();
    }

    fn scan_item(&self, input: ScanItemInput) -> Result<ScanItemOutput, anyhow::Error> {
        let page_id = self.lookup_table(input.table_id.into())?;
        let table_access = btree::Access::open(&self.bufmgr, page_id);
        let mut items = vec![];
        let mut buf = vec![];
        let mut count = 0;
        if input.backward {
            let mut iter = table_access.iter_rev(input.start.map(Into::into))?;
            while let Some(key) = iter.next(&mut buf)? {
                let key = key.into();
                let value = String::from_utf8(buf.clone())?;
                buf.clear();
                items.push(query::Item { key, value });
                count += 1;
                if count >= input.limit {
                    break;
                }
            }
        } else {
            let mut iter = table_access.iter(input.start.map(Into::into))?;
            while let Some(key) = iter.next(&mut buf)? {
                let key = key.into();
                let value = String::from_utf8(buf.clone())?;
                buf.clear();
                items.push(query::Item { key, value });
                count += 1;
                if count >= input.limit {
                    break;
                }
            }
        }
        Ok(ScanItemOutput { items })
    }

    fn create_table(&self, input: CreateTableInput) -> Result<CreateTableOutput, anyhow::Error> {
        let catalog = btree::Access::open(&self.bufmgr, PageId::CATALOG_PAGE_ID);
        let new_table = btree::Access::create(&self.bufmgr)?;
        let bytes: [u8; 8] = new_table.btree_page_id.into();
        catalog.put(input.table_id.into(), &bytes)?;
        Ok(CreateTableOutput)
    }

    fn flush(&self, _input: FlushInput) -> Result<query::FlushOutput, anyhow::Error> {
        self.bufmgr.flush();
        Ok(FlushOutput)
    }
}
