use std::{collections::HashMap, io, sync::Arc};

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::disk::{DiskManager, PageId, PAGE_SIZE};

pub type Page = [u8; PAGE_SIZE];

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BufferId(usize);

#[derive(Debug)]
pub struct Buffer {
    pub page: Page,
    pub is_dirty: bool,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page: [0u8; PAGE_SIZE],
            is_dirty: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct Frame {
    usage_count: u64,
    page_id: PageId,
    buffer: Arc<RwLock<Buffer>>,
}

pub struct BufferPool {
    page_table: HashMap<PageId, BufferId>,
    next_victim: usize,
    buffers: Vec<Frame>,
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let page_table = HashMap::new();
        let next_victim = 0;
        let mut buffers = vec![];
        buffers.resize_with(pool_size, Default::default);
        Self {
            page_table,
            next_victim,
            buffers,
        }
    }

    fn evict(&mut self) -> Option<(BufferId, &mut Frame)> {
        let pool_size = self.buffers.len();
        let mut consecutive_used = 0;
        let victim_idx = loop {
            let frame = &mut self.buffers[self.next_victim];
            if frame.usage_count == 0 {
                break self.next_victim;
            }
            if Arc::get_mut(&mut frame.buffer).is_some() {
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
        let frame = &mut self.buffers[victim_idx];
        frame.usage_count = 1;
        let victim_page_id = frame.page_id;
        self.page_table.remove(&victim_page_id);
        Some((BufferId(victim_idx), frame))
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

    pub fn fetch_page(&self, page_id: PageId) -> Result<Arc<RwLock<Buffer>>, Error> {
        let mut locked_pool = self.pool.lock();
        if let Some(&frame_id) = locked_pool.page_table.get(&page_id) {
            let frame = &mut locked_pool.buffers[frame_id.0];
            frame.usage_count += 1;
            return Ok(frame.buffer.clone());
        }
        let (frame_id, frame) = locked_pool.evict().ok_or(Error::NoFreeBuffer)?;
        let evict_page_id = frame.page_id;
        {
            let buffer = Arc::get_mut(&mut frame.buffer).unwrap().get_mut();
            let mut locked_disk = self.disk.lock();
            if buffer.is_dirty {
                locked_disk.write_page_data(evict_page_id, &buffer.page)?;
            }
            frame.page_id = page_id;
            buffer.is_dirty = false;
            locked_disk.read_page_data(page_id, &mut buffer.page)?;
        }
        let page = Arc::clone(&frame.buffer);
        locked_pool.page_table.remove(&evict_page_id);
        locked_pool.page_table.insert(page_id, frame_id);
        Ok(page)
    }

    pub fn create_page(&self) -> Result<(PageId, Arc<RwLock<Buffer>>), Error> {
        let mut locked_pool = self.pool.lock();
        let (frame_id, frame) = locked_pool.evict().ok_or(Error::NoFreeBuffer)?;
        let evict_page_id = frame.page_id;
        let page_id = {
            let buffer = Arc::get_mut(&mut frame.buffer).unwrap().get_mut();
            let mut locked_disk = self.disk.lock();
            if buffer.is_dirty {
                locked_disk.write_page_data(evict_page_id, &buffer.page)?;
            }
            let page_id = locked_disk.allocate_page();
            frame.page_id = page_id;
            *buffer = Buffer::default();
            buffer.is_dirty = true;
            page_id
        };
        let buffer = Arc::clone(&frame.buffer);
        locked_pool.page_table.remove(&evict_page_id);
        locked_pool.page_table.insert(page_id, frame_id);
        Ok((page_id, buffer))
    }

    pub fn flush(&self) -> Result<(), Error> {
        let locked_pool = self.pool.lock();
        let mut locked_disk = self.disk.lock();
        for (page_id, frame_id) in locked_pool.page_table.iter() {
            let frame = &locked_pool.buffers[frame_id.0];
            let mut rw_buffer = frame.buffer.write();
            locked_disk.write_page_data(*page_id, &rw_buffer.page)?;
            rw_buffer.is_dirty = false;
        }
        locked_disk.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

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
            let (page_id, buffer) = bufmgr.create_page().unwrap();
            assert!(bufmgr.create_page().is_err());
            let mut rw_buffer = buffer.write();
            rw_buffer.page.copy_from_slice(&hello);
            rw_buffer.is_dirty = true;
            page_id
        };
        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let ro_buffer = buffer.read();
            assert_eq!(&hello, &ro_buffer.page);
        }
        let page2_id = {
            let (page_id, buffer) = bufmgr.create_page().unwrap();
            let mut rw_buffer = buffer.write();
            rw_buffer.page.copy_from_slice(&world);
            rw_buffer.is_dirty = true;
            page_id
        };
        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let ro_buffer = buffer.read();
            assert_eq!(&hello, &ro_buffer.page);
        }
        {
            let buffer = bufmgr.fetch_page(page2_id).unwrap();
            let ro_buffer = buffer.read();
            assert_eq!(&world, &ro_buffer.page);
        }
    }
}
