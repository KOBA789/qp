use std::{ops::Range, convert::TryInto, mem::size_of, ops::{Deref, DerefMut}};

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use crate::disk::PageId;
use super::Key;

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    num_pairs: u16,
}

pub struct Pair<T> {
    data: T,
}

impl Pair<()> {
    const SIZE: usize = size_of::<Key>() + size_of::<PageId>();

    fn offset(index: usize) -> usize {
        index as usize * Self::SIZE
    }

    fn range(range: Range<usize>) -> Range<usize> {
        Self::offset(range.start)..Self::offset(range.end)
    }
}

impl<'a> Pair<&'a [u8]> {
    fn read(slice: &'a [u8], index: usize) -> Self {
        Pair {
            data: &slice[Pair::range(index..index + 1)]
        }
    }
}

impl<'a> Pair<&'a mut [u8]> {
    fn read_mut(slice: &'a mut [u8], index: usize) -> Self {
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

pub struct Branch<B> {
    header: LayoutVerified<B, Header>,
    body: B,
}

impl<B: ByteSlice> Branch<B> {
    pub fn new(bytes: B) -> Option<Self> {
        let (header, body) = LayoutVerified::new_from_prefix(bytes)?;
        Some(Self { header, body })
    }

    pub fn pair(&self, index: usize) -> Pair<&[u8]> {
        Pair::read(&self.body, index)
    }

    pub fn max_pairs(&self) -> usize {
        self.body.len() / Pair::SIZE
    }

    pub fn num_pairs(&self) -> usize {
        self.header.num_pairs as usize
    }

    pub fn find(&self, key: Key)  -> usize {
        use std::cmp::Ordering::{Equal, Greater};
        let mut base = 1usize;
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
            base - (cmp == Greater) as usize
        }
    }
}

impl<B: ByteSliceMut> Branch<B> {
    pub fn initialize(&mut self, key: Key, left_child: PageId, right_child: PageId) {
        self.header.num_pairs = 2;
        self.pair_mut(0).set_child(left_child);
        let mut right = self.pair_mut(1);
        right.set_key(key);
        right.set_child(right_child);
    }

    pub fn pair_mut(&mut self, index: usize) -> Pair<&mut [u8]> {
        Pair::read_mut(&mut self.body, index)
    }

    pub fn insert(&mut self, index: usize, key: Key, child: PageId) {
        let num_children = self.num_pairs();
        self.body.copy_within(Pair::range(index..num_children), Pair::offset(index + 1));
        let mut pair = self.pair_mut(index);
        pair.set_key(key);
        pair.set_child(child);
        self.header.num_pairs += 1;
    }

    pub fn split(&mut self, new_branch: &mut Branch<B>) -> Key {
        let num_keys = self.num_pairs();
        let mid = num_keys  / 2;
        let mid_key = self.pair(mid).key();
        let src = &self.body[Pair::range(mid..num_keys)];
        new_branch.body[0..src.len()].copy_from_slice(&src);
        new_branch.header.num_pairs = (num_keys - mid) as u16;
        self.header.num_pairs = (mid - 1) as u16;
        mid_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_find() {
        let mut data = vec![0u8; 100];
        let mut branch = Branch::new(data.as_mut_slice()).unwrap();
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
        let mut branch = Branch::new(data.as_mut_slice()).unwrap();
        branch.initialize(5u64.to_be_bytes(), PageId(1), PageId(2));
        branch.insert(2, 8u64.to_be_bytes(), PageId(3));
        branch.insert(3, 11u64.to_be_bytes(), PageId(4));
        let mut data2 = vec![0u8; 100];
        let mut branch2 = Branch::new(data2.as_mut_slice()).unwrap();
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
