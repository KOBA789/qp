use std::{ops::Range, convert::TryInto, mem::size_of, ops::{Deref, DerefMut}};

use crate::disk::PageId;
use super::Key;

pub struct Header<T> {
    data: T,
}

impl Header<()> {
    const SIZE: usize = 4;
}

impl<T> Header<T>
where
    T: Deref<Target = [u8]>,
{
    fn num_pairs(&self) -> u16 {
        u16::from_be_bytes(self.data[0..2].try_into().unwrap())
    }
}

impl<T> Header<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn set_num_pairs(&mut self, num_pairs: u16) {
        self.data[0..2].copy_from_slice(&num_pairs.to_be_bytes())
    }
}

pub struct Pair<T> {
    data: T,
}

impl Pair<()> {
    const SIZE: usize = size_of::<Key>() + size_of::<PageId>();

    fn offset(index: u16) -> usize {
        index as usize * Self::SIZE
    }

    fn range(range: Range<u16>) -> Range<usize> {
        Self::offset(range.start)..Self::offset(range.end)
    }
}

impl<'a> Pair<&'a [u8]> {
    fn read(slice: &'a [u8], index: u16) -> Self {
        Pair {
            data: &slice[Pair::range(index..index + 1)]
        }
    }
}

impl<'a> Pair<&'a mut [u8]> {
    fn read_mut(slice: &'a mut [u8], index: u16) -> Self {
        Pair {
            data: &mut slice[Pair::range(index..index + 1)]
        }
    }
}

impl<T> Pair<T>
where
    T: Deref<Target = [u8]>
{
    pub fn key(&self) -> Key {
        self.data[..size_of::<Key>()].try_into().unwrap()
    }

    pub fn child(&self) -> PageId {
        let bytes: [u8; 8] = self.data[size_of::<Key>()..].try_into().unwrap();
        bytes.into()
    }
}

impl<T> Pair<T>
where
    T: DerefMut<Target = [u8]>
{
    pub fn set_key(&mut self, key: Key) {
        self.data[..size_of::<Key>()].copy_from_slice(&key);
    }

    pub fn set_child(&mut self, child: PageId) {
        let bytes: [u8; 8] = child.into();
        self.data[size_of::<Key>()..].copy_from_slice(&bytes);
    }
}

pub struct Branch<T> {
    header: Header<T>,
    payload: T,
}

impl<'a> Branch<&'a [u8]> {
    pub fn new(data: &'a [u8]) -> Self {
        let (header, payload) = data.split_at(Header::SIZE);
        Self {
            header: Header { data: header },
            payload,
        }
    }
}

impl<'a> Branch<&'a mut [u8]> {
    pub fn new(data: &'a mut [u8]) -> Self {
        let (header, payload) = data.split_at_mut(Header::SIZE);
        Self {
            header: Header { data: header },
            payload,
        }
    }
}

impl<T> Branch<T>
where
    T: Deref<Target = [u8]>,
{
    pub fn pair(&self, index: u16) -> Pair<&[u8]> {
        Pair::read(&self.payload, index)
    }

    pub fn max_pairs(&self) -> u16 {
        (self.payload.len() / Pair::SIZE) as u16
    }

    pub fn num_pairs(&self) -> u16 {
        self.header.num_pairs()
    }

    pub fn find(&self, key: Key)  -> u16 {
        use std::cmp::Ordering::{Equal, Greater};
        let mut base = 1u16;
        let mut size = self.num_pairs() - 1;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            base = if self.pair(mid).key() > key {
                base
            } else {
                mid
            };
            size -= half;
        }
        let cmp = self.pair(base).key().cmp(&key);
        if cmp == Equal {
            base
        } else {
            base - (cmp == Greater) as u16
        }
    }
}

impl<T> Branch<T>
where
    T: DerefMut<Target = [u8]>,
{
    pub fn initialize(&mut self, key: Key, left_child: PageId, right_child: PageId) {
        self.header.set_num_pairs(2);
        self.pair_mut(0).set_child(left_child);
        let mut right = self.pair_mut(1);
        right.set_key(key);
        right.set_child(right_child);
    }

    pub fn pair_mut(&mut self, index: u16) -> Pair<&mut [u8]> {
        Pair::read_mut(&mut self.payload, index)
    }

    pub fn insert(&mut self, index: u16, key: Key, child: PageId) {
        let num_children = self.num_pairs();
        self.payload.copy_within(Pair::range(index..num_children), Pair::offset(index + 1));
        let mut pair = self.pair_mut(index);
        pair.set_key(key);
        pair.set_child(child);
        self.header.set_num_pairs(self.num_pairs() + 1);
    }

    pub fn split(&mut self, new_branch: &mut Branch<T>) -> Key {
        let num_keys = self.num_pairs();
        let mid = num_keys  / 2;
        let mid_key = self.pair(mid).key();
        let src = &self.payload[Pair::range(mid..num_keys)];
        new_branch.payload[0..src.len()].copy_from_slice(&src);
        new_branch.header.set_num_pairs(num_keys - mid);
        self.header.set_num_pairs(mid - 1);
        mid_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_find() {
        let mut data = vec![0u8; 100];
        let mut branch = Branch::<&mut _>::new(&mut data);
        branch.initialize(5u64.to_be_bytes(), PageId(1), PageId(2));
        branch.insert(2, 8u64.to_be_bytes(), PageId(3));
        branch.insert(3, 11u64.to_be_bytes(), PageId(4));
        assert_eq!(0, branch.find(1u64.to_be_bytes()));
        assert_eq!(1, branch.find(5u64.to_be_bytes()));
        assert_eq!(1, branch.find(6u64.to_be_bytes()));
        assert_eq!(2, branch.find(8u64.to_be_bytes()));
        assert_eq!(2, branch.find(10u64.to_be_bytes()));
        assert_eq!(3, branch.find(11u64.to_be_bytes()));
        assert_eq!(3, branch.find(12u64.to_be_bytes()));
    }

    #[test]
    fn test_split() {
        let mut data = vec![0u8; 100];
        let mut branch = Branch::<&mut _>::new(&mut data);
        branch.initialize(5u64.to_be_bytes(), PageId(1), PageId(2));
        branch.insert(2, 8u64.to_be_bytes(), PageId(3));
        branch.insert(3, 11u64.to_be_bytes(), PageId(4));
        let mut data2 = vec![0u8; 100];
        let mut branch2 = Branch::<&mut _>::new(&mut data2);
        let mid_key = branch.split(&mut branch2);
        assert_eq!(8u64.to_be_bytes(), mid_key);
        assert_eq!(0, branch.find(1u64.to_be_bytes()));
        assert_eq!(1, branch.find(5u64.to_be_bytes()));
        assert_eq!(1, branch.find(6u64.to_be_bytes()));
        assert_eq!(1, branch.find(8u64.to_be_bytes()));

        assert_eq!(0, branch2.find(9u64.to_be_bytes()));
        assert_eq!(1, branch2.find(11u64.to_be_bytes()));
        assert_eq!(1, branch2.find(12u64.to_be_bytes()));
    }
}
