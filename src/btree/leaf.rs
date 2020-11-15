use std::{
    convert::TryInto,
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::Key;
use crate::disk::PageId;
use crate::slotted::{self, Slotted};

pub struct Header<T> {
    data: T,
}

impl Header<()> {
    const SIZE: usize = 16;
}

impl<T> Header<T>
where
    T: Deref<Target = [u8]>,
{
    fn prev_page_id(&self) -> PageId {
        let bytes = self.data[0..8].try_into().unwrap();
        PageId(u64::from_be_bytes(bytes))
    }

    fn next_page_id(&self) -> PageId {
        let bytes = self.data[8..16].try_into().unwrap();
        PageId(u64::from_be_bytes(bytes))
    }
}

impl<T> Header<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn set_prev_page_id(&mut self, PageId(prev_page_id): PageId) {
        self.data[0..8].copy_from_slice(&prev_page_id.to_be_bytes());
    }

    fn set_next_page_id(&mut self, PageId(next_page_id): PageId) {
        self.data[8..16].copy_from_slice(&next_page_id.to_be_bytes());
    }
}

pub struct Record<T>(T);

impl<T> Record<T>
where
    T: Deref<Target = [u8]>,
{
    fn len(&self) -> u16 {
        self.0.len() as u16
    }

    pub fn key(&self) -> Key {
        self.0[..8].try_into().unwrap()
    }

    pub fn value(&self) -> &[u8] {
        &self.0[8..]
    }
}

impl<T> Record<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn set_key(&mut self, key: Key) {
        self.0[..8].copy_from_slice(&key)
    }

    fn value_mut(&mut self) -> &mut [u8] {
        &mut self.0[8..]
    }

    fn copy_from<U>(&mut self, record: &Record<U>)
    where
        U: Deref<Target = [u8]>,
    {
        self.0.clone_from_slice(&record.0)
    }
}

pub struct Leaf<T> {
    header: Header<T>,
    payload: Slotted<T>,
}

impl<'a> Leaf<&'a [u8]> {
    pub fn new(data: &'a [u8]) -> Self {
        let (header, payload) = data.split_at(Header::SIZE);
        Self {
            header: Header { data: header },
            payload: Slotted::<&[u8]>::new(payload),
        }
    }
}

impl<'a> Leaf<&'a mut [u8]> {
    pub fn new(data: &'a mut [u8]) -> Self {
        let (header, payload) = data.split_at_mut(Header::SIZE);
        Self {
            header: Header { data: header },
            payload: Slotted::<&mut [u8]>::new(payload),
        }
    }

    pub fn initialize(&mut self) {
        self.header.set_prev_page_id(PageId::INVALID_PAGE_ID);
        self.header.set_next_page_id(PageId::INVALID_PAGE_ID);
        self.payload.initialize();
    }
}

impl<T> Leaf<T>
where
    T: Deref<Target = [u8]>,
{
    pub fn prev_page_id(&self) -> Option<PageId> {
        self.header.prev_page_id().valid()
    }

    pub fn next_page_id(&self) -> Option<PageId> {
        self.header.next_page_id().valid()
    }

    pub fn num_records(&self) -> u16 {
        self.payload.num_slots()
    }

    pub fn find(&self, key: Key) -> Result<u16, u16> {
        use std::cmp::Ordering::{Equal, Less};
        if self.num_records() == 0 {
            return Err(0);
        }
        let mut base = 0u16;
        let mut size = self.num_records();
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            base = if self.record(mid).key() > key {
                base
            } else {
                mid
            };
            size -= half;
        }
        let cmp = self.record(base).key().cmp(&key);
        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as u16)
        }
    }

    pub fn get(&self, key: Key) -> Option<&[u8]> {
        let slot_id = self.find(key).ok()?;
        Some(&self.payload[slot_id][8..])
    }

    pub fn record(&self, index: u16) -> Record<&[u8]> {
        Record(&self.payload[index])
    }

    pub fn max_value_size(&self) -> usize {
        self.payload.inner().len() / 2 - size_of::<slotted::Pointer>() - size_of::<Key>()
    }
}

impl<T> Leaf<T>
where
    T: DerefMut<Target = [u8]>,
{
    pub fn set_prev_page_id(&mut self, prev_page_id: Option<PageId>) {
        self.header.set_prev_page_id(prev_page_id.into())
    }

    pub fn set_next_page_id(&mut self, next_page_id: Option<PageId>) {
        self.header.set_next_page_id(next_page_id.into())
    }

    fn record_mut(&mut self, index: u16) -> Record<&mut [u8]> {
        Record(&mut self.payload[index])
    }

    #[must_use = "insertion may fail"]
    pub fn put(&mut self, key: Key, value: &[u8]) -> bool {
        assert!(value.len() <= self.max_value_size());
        match self.find(key) {
            Ok(index) => {
                if self
                    .payload
                    .realloc(index, (size_of::<Key>() + value.len()) as u16)
                {
                    let mut record = self.record_mut(index);
                    record.set_key(key);
                    record.value_mut().copy_from_slice(value);
                    return true;
                }
            }
            Err(index) => {
                if self
                    .payload
                    .allocate(index, (size_of::<Key>() + value.len()) as u16)
                {
                    let mut record = self.record_mut(index);
                    record.set_key(key);
                    record.value_mut().copy_from_slice(value);
                    return true;
                }
            }
        }
        false
    }

    fn allocate_last(&mut self, len: u16) -> Record<&mut [u8]> {
        let next = self.num_records();
        assert!(self.payload.allocate(next, len));
        self.record_mut(next)
    }

    fn push_record(&mut self, record: &Record<&[u8]>) {
        self.allocate_last(record.len()).copy_from(record);
    }

    fn push_key_value(&mut self, key: Key, value: &[u8]) {
        let mut record = self.allocate_last((size_of::<Key>() + value.len()) as u16);
        record.set_key(key);
        record.value_mut().copy_from_slice(value);
    }

    pub fn split_put(&mut self, new_leaf: &mut Leaf<T>, new_key: Key, new_value: &[u8]) -> Key {
        use std::cmp::Ordering;
        loop {
            if self.payload.free_space() > new_leaf.payload.free_space() {
                break;
            }
            let num_records = self.num_records();
            if num_records <= 1 {
                break;
            }
            let last = num_records - 1;
            let record = self.record(last);
            let cmp = new_key.cmp(&record.key());
            if cmp == Ordering::Less {
                new_leaf.push_record(&record);
                self.payload.delete(last);
            } else {
                new_leaf.push_key_value(new_key, new_value);
                if cmp == Ordering::Equal {
                    self.payload.delete(last);
                }
                loop {
                    if self.payload.free_space() > new_leaf.payload.free_space() {
                        break;
                    }
                    let num_records = self.num_records();
                    if num_records <= 1 {
                        break;
                    }
                    let last = num_records - 1;
                    let record = self.record(last);
                    new_leaf.push_record(&record);
                    self.payload.delete(last);
                }
                new_leaf.payload.reverse();
                let first = new_leaf.record(0);
                return first.key();
            }
        }
        new_leaf.payload.reverse();
        assert!(self.put(new_key, new_value));
        let first = new_leaf.record(0);
        first.key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_find() {
        let mut page_data = vec![0; 100];
        let mut leaf_page = Leaf::<&mut _>::new(&mut page_data);
        leaf_page.initialize();
        assert!(leaf_page.payload.allocate(0, 8));
        assert!(leaf_page.payload.allocate(1, 8));
        assert!(leaf_page.payload.allocate(2, 8));
        leaf_page.payload[0].copy_from_slice(b"deadbeef");
        leaf_page.payload[1].copy_from_slice(b"deadbeeh");
        leaf_page.payload[2].copy_from_slice(b"deadbeek");
        assert_eq!(Ok(1), leaf_page.find(*b"deadbeeh"));
        assert_eq!(Err(1), leaf_page.find(*b"deadbeeg"));
        assert_eq!(Err(3), leaf_page.find(*b"deadbeez"));
    }

    #[test]
    fn test_leaf_insert() {
        let mut page_data = vec![0; 100];
        let mut leaf_page = Leaf::<&mut _>::new(&mut page_data);
        leaf_page.initialize();
        assert!(leaf_page.put(*b"deadbeef", b"world"));
        assert!(leaf_page.put(*b"facebook", b"!"));
        assert!(leaf_page.put(*b"beefdead", b"hello"));
        assert_eq!(Some(&b"hello"[..]), leaf_page.get(*b"beefdead"));
    }

    #[test]
    fn test_leaf_split_insert() {
        let mut page_data = vec![0; 54];
        let mut leaf_page = Leaf::<&mut _>::new(&mut page_data);
        leaf_page.initialize();
        assert!(leaf_page.put(*b"deadbeef", b"world"));
        assert!(leaf_page.put(*b"facebook", b"!"));
        assert!(!leaf_page.put(*b"beefdead", b"hello"));
        let mut new_page_data = vec![0; 54];
        let mut new_leaf_page = Leaf::<&mut _>::new(&mut new_page_data);
        new_leaf_page.initialize();
        leaf_page.split_put(&mut new_leaf_page, *b"beefdead", b"hello");
        assert_eq!(Some(&b"world"[..]), leaf_page.get(*b"deadbeef"));
    }
}
