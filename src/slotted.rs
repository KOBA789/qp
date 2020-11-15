use std::{convert::TryInto, ops::{Deref, DerefMut, Index, IndexMut, Range}, mem::size_of};

struct Header<T> {
    data: T,
}

impl Header<()> {
    const SIZE: usize = 4;
}

impl<T> Header<T>
where
    T: Deref<Target = [u8]>,
{
    fn num_slots(&self) -> u16 {
        let bytes: [u8; 2] = self.data[0..2].try_into().unwrap();
        u16::from_be_bytes(bytes)
    }

    fn free_space_offset(&self) -> u16 {
        let bytes: [u8; 2] = self.data[2..4].try_into().unwrap();
        u16::from_be_bytes(bytes)
    }
}

impl<T> Header<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn set_num_slot(&mut self, num_slot: u16) {
        self.data[0..2].copy_from_slice(&num_slot.to_be_bytes());
    }

    fn set_free_space_offset(&mut self, free_space_offset: u16) {
        self.data[2..4].copy_from_slice(&free_space_offset.to_be_bytes());
    }
}

pub struct Pointer {
    offset: u16,
    len: u16,
}

impl Pointer {
    fn offset(index: u16) -> usize {
        index as usize * size_of::<Self>()
    }

    fn range(range: Range<u16>) -> Range<usize> {
        Self::offset(range.start)..Self::offset(range.end)
    }

    fn read(slice: &[u8], index: u16) -> Self {
        let bytes: [u8; 4] = slice[Self::offset(index)..Self::offset(index + 1)].try_into().unwrap();
        bytes.into()
    }

    fn write(slice: &mut [u8], index: u16, pointer: Self) {
        let bytes: [u8; 4] = pointer.into();
        slice[Self::offset(index)..Self::offset(index + 1)].copy_from_slice(&bytes);
    }
}

impl From<[u8; 4]> for Pointer {
    fn from(bytes: [u8; 4]) -> Self {
        let offset = u16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let len = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
        Self { offset, len }
    }
}

impl From<Pointer> for [u8; 4] {
    fn from(Pointer { offset, len }: Pointer) -> Self {
        let mut bytes = [0u8; 4];
        bytes[0..2].copy_from_slice(&offset.to_be_bytes());
        bytes[2..4].copy_from_slice(&len.to_be_bytes());
        bytes
    }
}

pub struct Slotted<T> {
    header: Header<T>,
    payload: T,
}

impl<'a> Slotted<&'a [u8]> {
    pub fn new(data: &'a [u8]) -> Self {
        let (header_data, payload) = data.split_at(Header::SIZE);
        let header = Header { data: header_data };
        Self { header, payload }
    }
}

impl<'a> Slotted<&'a mut [u8]> {
    pub fn new(data: &'a mut [u8]) -> Self {
        let (header_data, payload) = data.split_at_mut(Header::SIZE);
        let header = Header { data: header_data };
        Self { header, payload }
    }
}

impl<T> Slotted<T>
where
    T: Deref<Target = [u8]>,
{
    pub fn num_slots(&self) -> u16 {
        self.header.num_slots()
    }

    pub fn free_space(&self) -> u16 {
        self.header.free_space_offset() - self.header.num_slots() * size_of::<Pointer>() as u16
    }

    fn pointer(&self, index: u16) -> Pointer {
        Pointer::read(&self.payload, index)
    }

    pub fn iter(&self) -> Iter<T> {
        Iter {
            slotted: &self,
            index: 0,
        }
    }

    pub fn inner(&self) -> &T {
        &self.payload
    }
}

impl<T> Slotted<T>
where
    T: DerefMut<Target = [u8]>,
{
    pub fn initialize(&mut self) {
        self.header.set_num_slot(0);
        self.header.set_free_space_offset(self.payload.len() as u16);
    }

    fn set_pointer(&mut self, index: u16, pointer: Pointer) {
        Pointer::write(&mut self.payload, index, pointer)
    }

    #[must_use = "allocation may fail"]
    pub fn allocate(&mut self, index: u16, element_len: u16) -> bool {
        assert!(index <= self.num_slots());

        // check whether free space is large enough or not
        if self.free_space() < element_len + size_of::<Pointer>() as u16 {
            return false;
        }

        let num_slots = self.num_slots();
        let element_offset = self.header.free_space_offset() - element_len;

        // extend pointers space
        self.header.set_num_slot(num_slots + 1);
        // extend elements space
        self.header.set_free_space_offset(element_offset);

        // shift pointers after index
        self.payload.copy_within(Pointer::range(index..num_slots), Pointer::offset(index + 1));

        // initialize pointer at index
        let mut pointer = self.pointer(index);
        pointer.len = element_len;
        pointer.offset = element_offset;
        self.set_pointer(index, pointer);

        true
    }

    pub fn delete(&mut self, index: u16) {
        assert!(index < self.num_slots());
        assert!(self.realloc(index, 0));
        let num_slots = self.num_slots();
        self.payload.copy_within(Pointer::range(index..num_slots), Pointer::offset(index + 1));
        self.header.set_num_slot(num_slots - 1);
    }

    #[must_use = "reallocation may fail"]
    pub fn realloc(&mut self, index: u16, new_element_len: u16) -> bool {
        assert!(index < self.num_slots());
        let Pointer { offset: org_element_offset, len: org_element_len } = self.pointer(index);
        let org_free_space_offset = self.header.free_space_offset();
        let diff = new_element_len as i16 - org_element_len as i16;

        if diff == 0 {
            return true;
        }
        if diff > 0 && self.free_space() < diff as u16 {
            // no space
            return false;
        }

        let new_free_space_offset = (org_free_space_offset as i16 - diff) as u16;
        self.header.set_free_space_offset(new_free_space_offset);

        let shift_range = (org_free_space_offset as usize)..(org_element_offset as usize);
        self.payload
            .copy_within(shift_range, new_free_space_offset as usize);

        for index in 0..self.num_slots() {
            let mut pointer = self.pointer(index);
            if pointer.offset <= org_element_offset {
                pointer.offset = (pointer.offset as i16 - diff) as u16;
                self.set_pointer(index, pointer);
            }
        }

        let mut pointer = self.pointer(index);
        pointer.len = new_element_len;
        if new_element_len == 0 {
            pointer.offset = new_free_space_offset;
        }
        self.set_pointer(index, pointer);

        true
    }

    pub fn reverse(&mut self) {
        let num_slots = self.num_slots();
        if num_slots == 0 {
            return;
        }
        let mut left = 0;
        let mut right = num_slots - 1;
        while left < right {
            let tmp = self.pointer(left);
            self.set_pointer(left, self.pointer(right));
            self.set_pointer(right, tmp);
            left += 1;
            right -= 1;
        }
    }
}

impl<T> Index<u16> for Slotted<T>
where
    T: Deref<Target = [u8]>,
{
    type Output = [u8];

    fn index(&self, index: u16) -> &Self::Output {
        let pointer = self.pointer(index);
        let offset = pointer.offset as usize;
        let len = pointer.len as usize;
        &self.payload[offset..offset + len]
    }
}

impl<T> IndexMut<u16> for Slotted<T>
where
    T: DerefMut<Target = [u8]>,
{
    fn index_mut(&mut self, index: u16) -> &mut Self::Output {
        let pointer = self.pointer(index);
        let offset = pointer.offset as usize;
        let len = pointer.len as usize;
        &mut self.payload[offset..offset + len]
    }
}

pub struct Iter<'a, T> {
    slotted: &'a Slotted<T>,
    index: u16,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Deref<Target = [u8]>,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.slotted.num_slots() {
            let index = self.index;
            self.index += 1;
            Some(&self.slotted[index])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut page_data = vec![0u8; 4096];
        let mut slotted = Slotted::<&mut [u8]>::new(&mut page_data);
        let insert = |slotted: &mut Slotted<&mut [u8]>, index: u16, buf: &[u8]| {
            assert!(slotted.allocate(index, buf.len() as u16));
            slotted[index].copy_from_slice(buf);
        };
        let push = |slotted: &mut Slotted<&mut [u8]>, buf: &[u8]| {
            let index = slotted.num_slots();
            insert(slotted, index, buf);
        };
        slotted.initialize();
        assert!(slotted.allocate(slotted.num_slots(), 5));
        push(&mut slotted, b"hello");
        push(&mut slotted, b"world");
        insert(&mut slotted, 1, b", ");
        push(&mut slotted, b"!");
        for elem in slotted.iter() {
            let s = std::str::from_utf8(elem).unwrap();
            print!("{}", s);
        }
    }
}
