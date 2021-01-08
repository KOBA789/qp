use std::{
    mem::size_of,
};

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use super::Key;
use crate::disk::PageId;
use crate::slotted::{self, Slotted};

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    prev_page_id: PageId,
    next_page_id: PageId,
}
pub struct Record<B> {
    key: LayoutVerified<B, Key>,
    pub value: B,
}

impl<B: ByteSlice> Record<B> {
    pub fn new(bytes: B) -> Option<Self> {
        let (key, value) = LayoutVerified::new_from_prefix(bytes)?;
        Some(Self { key, value })
    }

    pub fn len(&self) -> usize {
        size_of::<Key>() + self.value.len()
    }

    pub fn key(&self) -> Key {
        let mut key = Key::default();
        key.copy_from_slice(&self.key[..]);
        key
    }
}

pub struct Leaf<B> {
    header: LayoutVerified<B, Header>,
    body: Slotted<B>,
}

impl<B: ByteSlice> Leaf<B> {
    pub fn new(bytes: B) -> Option<Self> {
        let (header, body) = LayoutVerified::new_from_prefix(bytes)?;
        let body = Slotted::new(body)?;
        Some(Self { header, body })
    }

    pub fn prev_page_id(&self) -> Option<PageId> {
        self.header.prev_page_id.valid()
    }

    pub fn next_page_id(&self) -> Option<PageId> {
        self.header.next_page_id.valid()
    }

    pub fn num_records(&self) -> usize {
        self.body.num_slots()
    }

    pub fn find(&self, key: Key) -> Result<usize, usize> {
        use std::cmp::Ordering::{Equal, Less};
        if self.num_records() == 0 {
            return Err(0);
        }
        let mut base = 0;
        let mut size = self.num_records();
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            base = if self.record(mid).key.as_ref() > key.as_ref() {
                base
            } else {
                mid
            };
            size -= half;
        }
        let cmp = self.record(base).key.cmp(&key);
        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as usize)
        }
    }

    pub fn get(&self, key: Key) -> Option<&[u8]> {
        let slot_id = self.find(key).ok()?;
        Some(&self.record(slot_id).value)
    }

    pub fn record(&self, slot_id: usize) -> Record<&[u8]> {
        Record::new(&self.body[slot_id]).unwrap()
    }

    pub fn max_value_size(&self) -> usize {
        self.body.capacity() / 2 - size_of::<slotted::Pointer>() - size_of::<Key>()
    }
}

impl<B: ByteSliceMut> Leaf<B> {
    pub fn initialize(&mut self) {
        self.header.prev_page_id = PageId::INVALID_PAGE_ID;
        self.header.next_page_id = PageId::INVALID_PAGE_ID;
        self.body.initialize();
    }

    pub fn set_prev_page_id(&mut self, prev_page_id: Option<PageId>) {
        self.header.prev_page_id = prev_page_id.into()
    }

    pub fn set_next_page_id(&mut self, next_page_id: Option<PageId>) {
        self.header.next_page_id = next_page_id.into()
    }

    fn record_mut(&mut self, slot_id: usize) -> Record<&mut [u8]> {
        Record::new(&mut self.body[slot_id]).unwrap()
    }

    #[must_use = "insertion may fail"]
    pub fn put(&mut self, key: Key, value: &[u8]) -> bool {
        assert!(value.len() <= self.max_value_size());
        match self.find(key) {
            Ok(index) => {
                if self
                    .body
                    .resize(index, size_of::<Key>() + value.len())
                    .is_some()
                {
                    let mut record = self.record_mut(index);
                    record.key.copy_from_slice(&key);
                    record.value.copy_from_slice(value);
                    return true;
                }
            }
            Err(index) => {
                if self.body.insert(index, size_of::<Key>() + value.len()).is_some() {
                    let mut record = self.record_mut(index);
                    record.key.copy_from_slice(&key);
                    record.value.copy_from_slice(value);
                    return true;
                }
            }
        }
        false
    }

    fn allocate_last(&mut self, len: usize) -> Record<&mut [u8]> {
        let next = self.num_records();
        self.body.insert(next, len).unwrap();
        self.record_mut(next)
    }

    fn push_record(&mut self, record: &Record<&[u8]>) {
        let mut target = self.allocate_last(record.len());
        target.key.copy_from_slice(record.key.as_ref());
        target.value.copy_from_slice(record.value);
    }

    fn push_key_value(&mut self, key: Key, value: &[u8]) {
        let record = Record {
            key: LayoutVerified::new(&key[..]).unwrap(),
            value
        };
        self.push_record(&record);
    }

    pub fn split_put(&mut self, new_leaf: &mut Leaf<B>, new_key: Key, new_value: &[u8]) -> Key {
        use std::cmp::Ordering;
        loop {
            if self.body.free_space() > new_leaf.body.free_space() {
                break;
            }
            let num_records = self.num_records();
            if num_records <= 1 {
                break;
            }
            let last = num_records - 1;
            let record = self.record(last);
            let cmp = new_key.cmp(&record.key);
            if cmp == Ordering::Less {
                new_leaf.push_record(&record);
                self.body.remove(last);
            } else {
                new_leaf.push_key_value(new_key, new_value);
                if cmp == Ordering::Equal {
                    self.body.remove(last);
                }
                loop {
                    if self.body.free_space() > new_leaf.body.free_space() {
                        break;
                    }
                    let num_records = self.num_records();
                    if num_records <= 1 {
                        break;
                    }
                    let last = num_records - 1;
                    let record = self.record(last);
                    new_leaf.push_record(&record);
                    self.body.remove(last);
                }
                new_leaf.body.reverse();
                let first = new_leaf.record(0);
                return first.key();
            }
        }
        new_leaf.body.reverse();
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
        let mut leaf_page = Leaf::new(page_data.as_mut_slice()).unwrap();
        leaf_page.initialize();
        leaf_page.body.insert(0, 8).unwrap();
        leaf_page.body.insert(1, 8).unwrap();
        leaf_page.body.insert(2, 8).unwrap();
        leaf_page.body[0].copy_from_slice(b"deadbeef");
        leaf_page.body[1].copy_from_slice(b"deadbeeh");
        leaf_page.body[2].copy_from_slice(b"deadbeek");
        assert_eq!(Ok(1), leaf_page.find(*b"deadbeeh"));
        assert_eq!(Err(1), leaf_page.find(*b"deadbeeg"));
        assert_eq!(Err(3), leaf_page.find(*b"deadbeez"));
    }

    #[test]
    fn test_leaf_insert() {
        let mut page_data = vec![0; 100];
        let mut leaf_page = Leaf::new(page_data.as_mut_slice()).unwrap();
        leaf_page.initialize();
        assert!(leaf_page.put(*b"deadbeef", b"world"));
        assert!(leaf_page.put(*b"facebook", b"!"));
        assert!(leaf_page.put(*b"beefdead", b"hello"));
        assert_eq!(Some(&b"hello"[..]), leaf_page.get(*b"beefdead"));
    }

    #[test]
    fn test_leaf_split_insert() {
        let mut page_data = vec![0; 54];
        let mut leaf_page = Leaf::new(page_data.as_mut_slice()).unwrap();
        leaf_page.initialize();
        assert!(leaf_page.put(*b"deadbeef", b"world"));
        assert!(leaf_page.put(*b"facebook", b"!"));
        assert!(!leaf_page.put(*b"beefdead", b"hello"));
        let mut leaf_page = Leaf::new(page_data.as_mut_slice()).unwrap();
        let mut new_page_data = vec![0; 54];
        let mut new_leaf_page = Leaf::new(new_page_data.as_mut_slice()).unwrap();
        new_leaf_page.initialize();
        leaf_page.split_put(&mut new_leaf_page, *b"beefdead", b"hello");
        assert_eq!(Some(&b"world"[..]), leaf_page.get(*b"deadbeef"));
    }
}
