use std::{
    convert::TryInto,
    ops::{Deref, DerefMut},
};

use parking_lot::RawRwLock;
use thiserror::Error;

use crate::latch::OwnedRwLockExt;
use crate::{
    buffer::Page,
    buffer::{self, BufferPoolManager},
    latch::OwnedRwLockReadGuard,
    latch::OwnedRwLockWriteGuard,
};

use super::disk::PageId;

mod branch;
mod leaf;
mod node;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Buffer(#[from] buffer::Error),
    #[error("dead lock")]
    Deadlock,
}

struct BTreePage<T> {
    data: T,
}

impl<T> BTreePage<T>
where
    T: Deref<Target = [u8]>,
{
    fn root_page_id(&self) -> PageId {
        let bytes = self.data[0..8].try_into().unwrap();
        PageId(u64::from_be_bytes(bytes))
    }
}

impl<T> BTreePage<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn set_root_page_id(&mut self, PageId(prev_page_id): PageId) {
        self.data[0..8].copy_from_slice(&prev_page_id.to_be_bytes());
    }
}

pub type Key = [u8; 8];

pub struct Access<'a> {
    bufmgr: &'a BufferPoolManager,
    pub btree_page_id: PageId,
}

impl<'a> Access<'a> {
    pub fn create(bufmgr: &'a BufferPoolManager) -> Result<Self, Error> {
        let (btree_page_id, btree_page) = bufmgr.create_page()?;
        let mut btree_page = btree_page.write_owned();
        let mut btree = BTreePage {
            data: &mut btree_page.data[..],
        };
        let (root_page_id, root_page) = bufmgr.create_page()?;
        let mut root_page = root_page.write_owned();
        let mut root = node::NodePage::<&mut _>::new(&mut root_page.data);
        let mut leaf = root.initialize_as_leaf();
        leaf.initialize();
        btree.set_root_page_id(root_page_id);
        Ok(Self {
            bufmgr,
            btree_page_id,
        })
    }

    pub fn open(bufmgr: &'a BufferPoolManager, btree_page_id: PageId) -> Self {
        Self {
            bufmgr,
            btree_page_id,
        }
    }

    fn get_internal(
        &self,
        node_page: OwnedRwLockReadGuard<RawRwLock, Page>,
        key: Key,
        buf: &mut Vec<u8>,
    ) -> Result<bool, Error> {
        let node = node::NodePage::<&_>::new(&node_page.data);
        match node.node() {
            node::Node::Leaf(leaf) => Ok(leaf.get(key).map(|value| buf.extend(value)).is_some()),
            node::Node::Branch(branch) => {
                let index = branch.find(key);
                let child_page_id = branch.pair(index).child();
                let child_node_page = self.bufmgr.fetch_page(child_page_id)?.read_owned();
                drop(node_page);
                self.get_internal(child_node_page, key, buf)
            }
        }
    }

    pub fn get(&self, key: Key, buf: &mut Vec<u8>) -> Result<bool, Error> {
        let btree_page = self.bufmgr.fetch_page(self.btree_page_id)?.read_owned();
        let btree = BTreePage {
            data: &btree_page.data[..],
        };
        let root_page_id = btree.root_page_id();
        let root_page = self.bufmgr.fetch_page(root_page_id)?.read_owned();
        drop(btree_page);
        self.get_internal(root_page, key, buf)
    }

    fn iter_internal(
        &self,
        node_page: OwnedRwLockReadGuard<RawRwLock, Page>,
        key: Option<Key>,
    ) -> Result<Iter<'a>, Error> {
        let node = node::NodePage::<&_>::new(&node_page.data);
        match node.node() {
            node::Node::Leaf(leaf) => {
                let start = key
                    .map(|key| leaf.find(key).unwrap_or_else(|index| index))
                    .unwrap_or(0);
                Ok(Iter {
                    bufmgr: &self.bufmgr,
                    index: start,
                    page: Some(node_page),
                })
            }
            node::Node::Branch(branch) => {
                let index = key.map(|key| branch.find(key)).unwrap_or(0);
                let child_page_id = branch.pair(index).child();
                let child_node_page = self.bufmgr.fetch_page(child_page_id)?.read_owned();
                drop(node_page);
                self.iter_internal(child_node_page, key)
            }
        }
    }

    pub fn iter(&self, key: Option<Key>) -> Result<Iter<'a>, Error> {
        let btree_page = self.bufmgr.fetch_page(self.btree_page_id)?.read_owned();
        let btree = BTreePage {
            data: &btree_page.data[..],
        };
        let root_page_id = btree.root_page_id();
        let root_page = self.bufmgr.fetch_page(root_page_id)?.read_owned();
        drop(btree_page);
        self.iter_internal(root_page, key)
    }

    fn iter_rev_internal(
        &self,
        node_page: OwnedRwLockReadGuard<RawRwLock, Page>,
        key: Option<Key>,
    ) -> Result<IterRev<'a>, Error> {
        let node = node::NodePage::<&_>::new(&node_page.data);
        match node.node() {
            node::Node::Leaf(leaf) => {
                let start = key
                    .map(|key| {
                        leaf.find(key)
                            .map(|index| index as i16)
                            .unwrap_or_else(|index| index as i16 - 1)
                    })
                    .unwrap_or_else(|| leaf.num_records() as i16 - 1);
                Ok(IterRev {
                    bufmgr: &self.bufmgr,
                    index: start,
                    page: Some(node_page),
                })
            }
            node::Node::Branch(branch) => {
                let index = key.map(|key| branch.find(key)).unwrap_or_else(|| branch.num_pairs() - 1);
                let child_page_id = branch.pair(index).child();
                let child_node_page = self.bufmgr.fetch_page(child_page_id)?.read_owned();
                drop(node_page);
                self.iter_rev_internal(child_node_page, key)
            }
        }
    }

    pub fn iter_rev(&self, key: Option<Key>) -> Result<IterRev<'a>, Error> {
        let btree_page = self.bufmgr.fetch_page(self.btree_page_id)?.read_owned();
        let btree = BTreePage {
            data: &btree_page.data[..],
        };
        let root_page_id = btree.root_page_id();
        let root_page = self.bufmgr.fetch_page(root_page_id)?.read_owned();
        drop(btree_page);
        self.iter_rev_internal(root_page, key)
    }

    fn put_internal(
        &self,
        node_page_id: PageId,
        mut page: OwnedRwLockWriteGuard<RawRwLock, Page>,
        key: Key,
        value: &[u8],
    ) -> Result<Option<(Key, PageId)>, Error> {
        let mut node = node::NodePage::<&mut _>::new(&mut page.data);
        match node.node_mut() {
            node::Node::Leaf(mut leaf) => {
                if leaf.put(key, value) {
                    page.is_dirty = true;
                    Ok(None)
                } else {
                    let next_leaf_page_id = leaf.next_page_id();
                    let next_leaf_page = next_leaf_page_id
                        .map(|next_leaf_page_id| {
                            self.bufmgr
                                .fetch_page(next_leaf_page_id)?
                                .try_write_owned()
                                .map(Ok)
                                .unwrap_or(Err(Error::Deadlock))
                        })
                        .transpose()?;

                    let (new_leaf_page_id, new_leaf_page) = self.bufmgr.create_page()?;

                    if let Some(mut next_leaf_page) = next_leaf_page {
                        let mut node_page = node::NodePage::<&mut _>::new(&mut next_leaf_page.data);
                        let mut next_leaf = node_page.node_mut().try_into_leaf().ok().unwrap();
                        next_leaf.set_prev_page_id(Some(new_leaf_page_id));
                    }
                    leaf.set_next_page_id(Some(new_leaf_page_id));

                    let mut new_leaf_page = new_leaf_page.write_owned();
                    let mut new_leaf_node_page =
                        node::NodePage::<&mut _>::new(&mut new_leaf_page.data);
                    let mut new_leaf = new_leaf_node_page.initialize_as_leaf();
                    new_leaf.initialize();
                    let new_leaf_first_key = leaf.split_put(&mut new_leaf, key, value);
                    new_leaf.set_prev_page_id(Some(node_page_id));
                    new_leaf.set_next_page_id(next_leaf_page_id);
                    page.is_dirty = true;
                    Ok(Some((new_leaf_first_key, new_leaf_page_id)))
                }
            }
            node::Node::Branch(mut branch) => {
                let index = branch.find(key);
                let child_page_id = branch.pair(index).child();
                let child_node_page = self.bufmgr.fetch_page(child_page_id)?.write_owned();
                if let Some((key, child)) =
                    self.put_internal(child_page_id, child_node_page, key, value)?
                {
                    branch.insert(index + 1, key, child);
                    if branch.max_pairs() <= branch.num_pairs() {
                        let (new_branch_page_id, new_branch_page) = self.bufmgr.create_page()?;
                        let mut new_branch_page = new_branch_page.write_owned();
                        let mut new_branch_node_page =
                            node::NodePage::<&mut _>::new(&mut new_branch_page.data);
                        let mut new_branch = new_branch_node_page.initialize_as_branch();
                        let overflow_key = branch.split(&mut new_branch);
                        page.is_dirty = true;
                        Ok(Some((overflow_key, new_branch_page_id)))
                    } else {
                        page.is_dirty = true;
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn put(&self, key: Key, value: &[u8]) -> Result<(), Error> {
        let mut btree_page = self.bufmgr.fetch_page(self.btree_page_id)?.write_owned();
        let mut btree = BTreePage {
            data: &mut btree_page.data[..],
        };
        let root_page_id = btree.root_page_id();
        let root_page = self.bufmgr.fetch_page(root_page_id)?.write_owned();
        if let Some((key, child)) = self.put_internal(root_page_id, root_page, key, value)? {
            let (new_root_page_id, new_root_page) = self.bufmgr.create_page()?;
            let mut new_root_page = new_root_page.write_owned();
            let mut node_page = node::NodePage::<&mut _>::new(&mut new_root_page.data);
            let mut branch = node_page.initialize_as_branch();
            branch.initialize(key, root_page_id, child);
            btree.set_root_page_id(new_root_page_id);
            btree_page.is_dirty = true;
        }
        Ok(())
    }
}

pub struct Iter<'a> {
    bufmgr: &'a BufferPoolManager,
    page: Option<OwnedRwLockReadGuard<RawRwLock, Page>>,
    index: u16,
}
impl<'a> Iter<'a> {
    pub fn next(&mut self, buf: &mut Vec<u8>) -> Result<Option<Key>, Error> {
        if let Some(page) = &self.page {
            let node_page = node::NodePage::<&_>::new(&page.data);
            let leaf = node_page.node().try_into_leaf().ok().unwrap();
            if self.index < leaf.num_records() {
                let record = leaf.record(self.index);
                self.index += 1;
                buf.extend(record.value());
                Ok(Some(record.key()))
            } else {
                self.page = match leaf.next_page_id() {
                    Some(next_page_id) => Some(self.bufmgr.fetch_page(next_page_id)?.read_owned()),
                    None => None,
                };
                self.index = 0;
                self.next(buf)
            }
        } else {
            Ok(None)
        }
    }
}

pub struct IterRev<'a> {
    bufmgr: &'a BufferPoolManager,
    page: Option<OwnedRwLockReadGuard<RawRwLock, Page>>,
    index: i16,
}
impl<'a> IterRev<'a> {
    pub fn next(&mut self, buf: &mut Vec<u8>) -> Result<Option<Key>, Error> {
        if let Some(page) = &self.page {
            let node_page = node::NodePage::<&_>::new(&page.data);
            let leaf = node_page.node().try_into_leaf().ok().unwrap();
            if self.index >= 0 {
                let record = leaf.record(self.index as u16);
                self.index -= 1;
                buf.extend(record.value());
                Ok(Some(record.key()))
            } else {
                self.page = match leaf.prev_page_id() {
                    Some(prev_page_id) => {
                        let prev_page = self.bufmgr.fetch_page(prev_page_id)?.read_owned();
                        let node_page = node::NodePage::<&_>::new(&prev_page.data);
                        let leaf = node_page.node().try_into_leaf().ok().unwrap();
                        self.index = leaf.num_records() as i16 - 1;
                        Some(prev_page)
                    }
                    None => None,
                };
                self.next(buf)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{buffer::BufferPool, disk::DiskManager};

    use super::*;
    #[test]
    fn test() {
        std::fs::remove_file("btree_test.qp").ok();
        let disk = DiskManager::open("btree_test.qp").unwrap();
        let pool = BufferPool::new(10);
        let bufmgr = BufferPoolManager::new(disk, pool);
        let btree_access = Access::create(&bufmgr).unwrap();
        btree_access.put(6u64.to_be_bytes(), b"world").unwrap();
        btree_access.put(3u64.to_be_bytes(), b"hello").unwrap();
        btree_access.put(8u64.to_be_bytes(), b"!").unwrap();
        btree_access.put(4u64.to_be_bytes(), b",").unwrap();

        let mut buf = vec![];
        assert!(btree_access.get(3u64.to_be_bytes(), &mut buf).unwrap());
        assert_eq!(b"hello", &*buf);
        buf.clear();
        assert!(btree_access.get(8u64.to_be_bytes(), &mut buf).unwrap());
        assert_eq!(b"!", &*buf);
        buf.clear();
    }

    #[test]
    fn test_split() {
        std::fs::remove_file("btree_test2.qp").ok();
        let disk = DiskManager::open("btree_test2.qp").unwrap();
        let pool = BufferPool::new(10);
        let bufmgr = BufferPoolManager::new(disk, pool);
        let btree_access = Access::create(&bufmgr).unwrap();
        let long_padding = vec![0xDEu8; 1500];
        btree_access.put(6u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(3u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(8u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(4u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(5u64.to_be_bytes(), b"hello").unwrap();

        let mut buf = vec![];
        assert!(btree_access.get(5u64.to_be_bytes(), &mut buf).unwrap());
        assert_eq!(b"hello", &*buf);
        buf.clear();
    }

    #[test]
    fn test_iter() {
        std::fs::remove_file("btree_test3.qp").ok();
        let disk = DiskManager::open("btree_test2.qp").unwrap();
        let pool = BufferPool::new(10);
        let bufmgr = BufferPoolManager::new(disk, pool);
        let btree_access = Access::create(&bufmgr).unwrap();
        let long_padding = vec![0xDEu8; 1500];
        btree_access.put(6u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(3u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(8u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(4u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(5u64.to_be_bytes(), b"hello").unwrap();

        let mut iter = btree_access.iter(Some(4u64.to_be_bytes())).unwrap();
        let mut buf = vec![];
        assert_eq!(Some(4u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(&long_padding, &buf);
        buf.clear();
        assert_eq!(Some(5u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(b"hello", &*buf);
        buf.clear();
        assert_eq!(Some(6u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(&long_padding, &buf);
        buf.clear();
        assert_eq!(Some(8u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(&long_padding, &buf);
        buf.clear();
        assert_eq!(None, iter.next(&mut buf).unwrap());
    }

    #[test]
    fn test_rev_iter() {
        std::fs::remove_file("btree_test4.qp").ok();
        let disk = DiskManager::open("btree_test2.qp").unwrap();
        let pool = BufferPool::new(10);
        let bufmgr = BufferPoolManager::new(disk, pool);
        let btree_access = Access::create(&bufmgr).unwrap();
        let long_padding = vec![0xDEu8; 1500];
        btree_access.put(6u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(3u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(8u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(4u64.to_be_bytes(), &long_padding).unwrap();
        btree_access.put(5u64.to_be_bytes(), b"hello").unwrap();

        let mut iter = btree_access.iter_rev(Some(7u64.to_be_bytes())).unwrap();
        let mut buf = vec![];
        assert_eq!(Some(6u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(&long_padding, &buf);
        buf.clear();
        assert_eq!(Some(5u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(b"hello", &*buf);
        buf.clear();
        assert_eq!(Some(4u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(&long_padding, &buf);
        buf.clear();
        assert_eq!(Some(3u64.to_be_bytes()), iter.next(&mut buf).unwrap());
        assert_eq!(&long_padding, &buf);
        buf.clear();
        assert_eq!(None, iter.next(&mut buf).unwrap());
    }
}
