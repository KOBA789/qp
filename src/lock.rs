use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use parking_lot::lock_api::{RawRwLock, RwLock};

pub trait OwnedRwLockExt<R, T>
where
    R: RawRwLock,
{
    unsafe fn raw(&self) -> &R;
    unsafe fn read_guard_owned(self: Arc<Self>) -> OwnedRwLockReadGuard<R, T>;
    unsafe fn write_guard_owned(self: Arc<Self>) -> OwnedRwLockWriteGuard<R, T>;

    fn read_owned(self: Arc<Self>) -> OwnedRwLockReadGuard<R, T> {
        unsafe { self.raw().lock_shared() };
        unsafe { self.read_guard_owned() }
    }

    fn write_owned(self: Arc<Self>) -> OwnedRwLockWriteGuard<R, T> {
        unsafe { self.raw().lock_exclusive() };
        unsafe { self.write_guard_owned() }
    }

    fn try_read_owned(self: Arc<Self>) -> Option<OwnedRwLockReadGuard<R, T>> {
        if unsafe { self.raw().try_lock_shared() } {
            Some(unsafe { self.read_guard_owned() })
        } else {
            None
        }
    }

    fn try_write_owned(self: Arc<Self>) -> Option<OwnedRwLockWriteGuard<R, T>> {
        if unsafe { self.raw().try_lock_exclusive() } {
            Some(unsafe { self.write_guard_owned() })
        } else {
            None
        }
    }
}

impl<R, T> OwnedRwLockExt<R, T> for RwLock<R, T>
where
    R: RawRwLock,
{
    unsafe fn raw(&self) -> &R {
        RwLock::raw(self)
    }

    unsafe fn read_guard_owned(self: Arc<Self>) -> OwnedRwLockReadGuard<R, T> {
        OwnedRwLockReadGuard {
            rwlock: self,
            marker: PhantomData,
        }
    }

    unsafe fn write_guard_owned(self: Arc<Self>) -> OwnedRwLockWriteGuard<R, T> {
        OwnedRwLockWriteGuard {
            rwlock: self,
            marker: PhantomData,
        }
    }
}

pub struct OwnedRwLockReadGuard<R, T>
where
    R: RawRwLock,
{
    rwlock: Arc<RwLock<R, T>>,
    marker: PhantomData<R::GuardMarker>,
}
unsafe impl<R: RawRwLock, T: Sync> Sync for OwnedRwLockReadGuard<R, T> {}

impl<R, T> Deref for OwnedRwLockReadGuard<R, T>
where
    R: RawRwLock,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.rwlock
                .data_ptr()
                .as_ref()
                .expect("Arc must not point to null")
        }
    }
}

impl<R, T> Drop for OwnedRwLockReadGuard<R, T>
where
    R: RawRwLock,
{
    fn drop(&mut self) {
        unsafe {
            self.rwlock.raw().unlock_shared();
        }
    }
}

pub struct OwnedRwLockWriteGuard<R, T>
where
    R: RawRwLock,
{
    rwlock: Arc<RwLock<R, T>>,
    marker: PhantomData<R::GuardMarker>,
}
unsafe impl<R: RawRwLock, T: Sync> Sync for OwnedRwLockWriteGuard<R, T> {}

impl<R, T> Deref for OwnedRwLockWriteGuard<R, T>
where
    R: RawRwLock,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.rwlock
                .data_ptr()
                .as_ref()
                .expect("Arc must not point to null")
        }
    }
}

impl<R, T> DerefMut for OwnedRwLockWriteGuard<R, T>
where
    R: RawRwLock,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.rwlock
                .data_ptr()
                .as_mut()
                .expect("Arc must not point to null")
        }
    }
}

impl<R, T> Drop for OwnedRwLockWriteGuard<R, T>
where
    R: RawRwLock,
{
    fn drop(&mut self) {
        unsafe {
            self.rwlock.raw().unlock_exclusive();
        }
    }
}
