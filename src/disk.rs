use std::{
    convert::{TryFrom, TryInto},
    io::{prelude::*, SeekFrom},
};
use std::{fs::File, fs::OpenOptions, path::Path};

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct PageId(pub u64);
impl PageId {
    pub const CATALOG_PAGE_ID: PageId = PageId(0);
    pub const INVALID_PAGE_ID: PageId = PageId(u64::MAX);

    pub fn valid(self) -> Option<PageId> {
        if self == Self::INVALID_PAGE_ID {
            None
        } else {
            Some(self)
        }
    }
}
impl Default for PageId {
    fn default() -> Self {
        Self::INVALID_PAGE_ID
    }
}
impl From<[u8; 8]> for PageId {
    fn from(bytes: [u8; 8]) -> Self {
        PageId(u64::from_be_bytes(bytes))
    }
}
impl From<PageId> for [u8; 8] {
    fn from(page_id: PageId) -> Self {
        page_id.0.to_be_bytes()
    }
}
impl From<Option<PageId>> for PageId {
    fn from(page_id: Option<PageId>) -> Self {
        page_id.unwrap_or_default()
    }
}
impl<'a> TryFrom<&'a [u8]> for PageId {
    type Error = std::array::TryFromSliceError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let array: [u8; 8] = value.try_into()?;
        Ok(array.into())
    }
}

pub struct DiskManager {
    data_file: File,
    next_page_id: u64,
}

impl DiskManager {
    pub fn new(data_file: File) -> std::io::Result<Self> {
        let next_page_id = data_file.metadata()?.len() / PAGE_SIZE as u64;
        Ok(Self {
            data_file,
            next_page_id,
        })
    }

    pub fn open(data_file_path: impl AsRef<Path>) -> std::io::Result<Self> {
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_file_path)?;
        Self::new(data_file)
    }

    pub fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> std::io::Result<()> {
        let offset = PAGE_SIZE as u64 * page_id.0;
        self.data_file.seek(SeekFrom::Start(offset))?;
        self.data_file.read_exact(data)
    }

    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> std::io::Result<()> {
        let offset = PAGE_SIZE as u64 * page_id.0;
        self.data_file.seek(SeekFrom::Start(offset))?;
        self.data_file.write_all(data)?;
        self.data_file.flush()?;
        self.data_file.sync_all()
    }

    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test() {
        let (data_file, data_file_path) = NamedTempFile::new().unwrap().into_parts();
        let mut disk = DiskManager::new(data_file).unwrap();
        let mut hello = Vec::with_capacity(PAGE_SIZE);
        hello.extend_from_slice(b"hello");
        hello.resize(PAGE_SIZE, 0);
        let hello_page_id = disk.allocate_page();
        disk.write_page_data(hello_page_id, &hello).unwrap();
        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);
        let world_page_id = disk.allocate_page();
        disk.write_page_data(world_page_id, &world).unwrap();
        drop(disk);
        let mut disk2 = DiskManager::open(&data_file_path).unwrap();
        let mut buf = vec![0; PAGE_SIZE];
        disk2.read_page_data(hello_page_id, &mut buf).unwrap();
        assert_eq!(hello, buf);
        disk2.read_page_data(world_page_id, &mut buf).unwrap();
        assert_eq!(world, buf);
    }
}
