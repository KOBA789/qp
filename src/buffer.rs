use std::{collections::HashMap, io, sync::Arc};

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::disk::{DiskManager, PageId, PAGE_SIZE};

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct FrameId(usize);

#[derive(Debug)]
pub struct Page {
    pub is_dirty: bool,
    pub data: Vec<u8>,
}

impl Default for Page {
    fn default() -> Self {
        Self {
            is_dirty: false,
            data: vec![0u8; PAGE_SIZE],
        }
    }
}

#[derive(Debug, Default)]
pub struct Frame {
    page_id: PageId,
    usage_count: u64,
    body: Arc<RwLock<Page>>,
}

pub struct BufferPool {
    page_table: HashMap<PageId, FrameId>,
    next_victim: usize,
    frames: Vec<Frame>,
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let page_table = HashMap::new();
        let next_victim = 0;
        let mut frames = vec![];
        frames.resize_with(pool_size, Default::default);
        Self {
            page_table,
            next_victim,
            frames,
        }
    }

    fn evict(&mut self) -> Option<(FrameId, &mut Frame)> {
        let pool_size = self.frames.len();
        let mut consecutive_used = 0;
        let victim_idx = loop {
            let frame = &mut self.frames[self.next_victim];
            if frame.usage_count == 0 {
                break self.next_victim;
            }
            if Arc::get_mut(&mut frame.body).is_some() {
                frame.usage_count -= 1;
                consecutive_used = 0;
            } else {
                consecutive_used += 1;
                if consecutive_used >= pool_size {
                    return None;
                }
            }
            self.next_victim = (self.next_victim + 1) % pool_size;
        };
        let frame = &mut self.frames[victim_idx];
        frame.usage_count = 1;
        let victim_page_id = frame.page_id;
        self.page_table.remove(&victim_page_id);
        Some((FrameId(victim_idx), frame))
    }
}

pub struct BufferPoolManager {
    disk: Mutex<DiskManager>,
    pool: Mutex<BufferPool>,
}

impl BufferPoolManager {
    pub fn new(disk: DiskManager, pool: BufferPool) -> Self {
        Self {
            disk: Mutex::new(disk),
            pool: Mutex::new(pool),
        }
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<Arc<RwLock<Page>>, Error> {
        let mut locked_pool = self.pool.lock();
        if let Some(&frame_id) = locked_pool.page_table.get(&page_id) {
            let frame = &mut locked_pool.frames[frame_id.0];
            frame.usage_count += 1;
            return Ok(frame.body.clone());
        }
        let (frame_id, frame) = locked_pool.evict().ok_or(Error::NoFreeBuffer)?;
        let evict_page_id = frame.page_id;
        {
            let page = Arc::get_mut(&mut frame.body).unwrap().get_mut();
            let mut locked_disk = self.disk.lock();
            if page.is_dirty {
                locked_disk.write_page_data(evict_page_id, &page.data)?;
            }
            frame.page_id = page_id;
            page.is_dirty = false;
            locked_disk.read_page_data(page_id, &mut page.data)?;
        }
        let page = Arc::clone(&frame.body);
        locked_pool.page_table.remove(&evict_page_id);
        locked_pool.page_table.insert(page_id, frame_id);
        Ok(page)
    }

    pub fn create_page(&self) -> Result<(PageId, Arc<RwLock<Page>>), Error> {
        let mut locked_pool = self.pool.lock();
        let (frame_id, frame) = locked_pool.evict().ok_or(Error::NoFreeBuffer)?;
        let evict_page_id = frame.page_id;
        let page_id = {
            let page = Arc::get_mut(&mut frame.body).unwrap().get_mut();
            let mut locked_disk = self.disk.lock();
            if page.is_dirty {
                locked_disk.write_page_data(evict_page_id, &page.data)?;
            }
            let page_id = locked_disk.allocate_page();
            frame.page_id = page_id;
            *page = Page::default();
            page.is_dirty = true;
            page_id
        };
        let page = Arc::clone(&frame.body);
        locked_pool.page_table.remove(&evict_page_id);
        locked_pool.page_table.insert(page_id, frame_id);
        Ok((page_id, page))
    }

    pub fn flush(&self) -> Result<(), Error> {
        let locked_pool = self.pool.lock();
        let mut locked_disk = self.disk.lock();
        for (page_id, frame_id) in locked_pool.page_table.iter() {
            let frame = &locked_pool.frames[frame_id.0];
            let mut rw_page = frame.body.write();
            locked_disk
                .write_page_data(*page_id, &rw_page.data)?;
            rw_page.is_dirty = false;
        }
        locked_disk.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;
    use super::*;

    #[test]
    fn test() {
        let mut hello = Vec::with_capacity(PAGE_SIZE);
        hello.extend_from_slice(b"hello");
        hello.resize(PAGE_SIZE, 0);
        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);

        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(1);
        let bufmgr = BufferPoolManager::new(disk, pool);
        let page1_id = {
            let (page_id, page) = bufmgr.create_page().unwrap();
            assert!(bufmgr.create_page().is_err());
            let mut rw_page = page.write();
            rw_page.data.copy_from_slice(&hello);
            rw_page.is_dirty = true;
            page_id
        };
        {
            let page = bufmgr.fetch_page(page1_id).unwrap();
            let ro_page = page.read();
            assert_eq!(&hello, &ro_page.data);
        }
        let page2_id = {
            let (page_id, page) = bufmgr.create_page().unwrap();
            let mut rw_page = page.write();
            rw_page.data.copy_from_slice(&world);
            rw_page.is_dirty = true;
            page_id
        };
        {
            let page = bufmgr.fetch_page(page1_id).unwrap();
            let ro_page = page.read();
            assert_eq!(&hello, &ro_page.data);
        }
        {
            let page = bufmgr.fetch_page(page2_id).unwrap();
            let ro_page = page.read();
            assert_eq!(&world, &ro_page.data);
        }
    }
}
