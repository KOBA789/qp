use std::ops::{Deref, DerefMut};

use super::branch::Branch;
use super::leaf::Leaf;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum NodeType {
    Leaf = 1,
    Branch = 2,
}

pub struct Header<T> {
    data: T,
}

impl Header<()> {
    const SIZE: usize = 4;
}

impl<T> Header<T>
where
    T: Deref<Target = [u8]>
{
    fn node_type(&self) -> NodeType {
        let byte = self.data[0];
        if byte == NodeType::Leaf as u8 {
            return NodeType::Leaf;
        }
        if byte == NodeType::Branch as u8 {
            return NodeType::Branch;
        }
        unreachable!()
    }
}

impl<T> Header<T>
where
    T: DerefMut<Target = [u8]>
{
    fn set_node_type(&mut self, node_type: NodeType) {
        self.data[0] = node_type as u8;
    }
}

pub struct NodePage<T> {
    header: Header<T>,
    payload: T,
}

impl<'a> NodePage<&'a [u8]> {
    pub fn new(data: &'a [u8]) -> Self {
        let (header, payload) = data.split_at(Header::SIZE);
        Self {
            header: Header { data: header },
            payload,
        }
    }
}

impl<'a> NodePage<&'a mut [u8]> {
    pub fn new(data: &'a mut [u8]) -> Self {
        let (header, payload) = data.split_at_mut(Header::SIZE);
        Self {
            header: Header { data: header },
            payload,
        }
    }
}

impl<T> NodePage<T>
where
    T: Deref<Target = [u8]>
{
    pub fn node(&self) -> Node<&[u8]> {
        match self.header.node_type() {
            NodeType::Leaf => Node::Leaf(Leaf::<&_>::new(&self.payload)),
            NodeType::Branch => Node::Branch(Branch::<&_>::new(&self.payload)),
        }
    }
}

impl<T> NodePage<T>
where
    T: DerefMut<Target = [u8]>
{
    pub fn initialize_as_leaf(&mut self) -> Leaf<&mut [u8]> {
        self.header.set_node_type(NodeType::Leaf);
        Leaf::<&mut _>::new(&mut self.payload)
    }

    pub fn initialize_as_branch(&mut self) -> Branch<&mut [u8]> {
        self.header.set_node_type(NodeType::Branch);
        Branch::<&mut _>::new(&mut self.payload)
    }

    pub fn node_mut(&mut self) -> Node<&mut [u8]> {
        match self.header.node_type() {
            NodeType::Leaf => Node::Leaf(Leaf::<&mut _>::new(&mut self.payload)),
            NodeType::Branch => Node::Branch(Branch::<&mut _>::new(&mut self.payload)),
        }
    }
}

pub enum Node<T> {
    Leaf(Leaf<T>),
    Branch(Branch<T>),
}

impl<T> Node<T> {
    pub fn try_into_leaf(self) -> Result<Leaf<T>, Self> {
        match self {
            Node::Leaf(leaf) => Ok(leaf),
            _ => Err(self),
        }
    }

    pub fn try_into_branch(self) -> Result<Branch<T>, Self> {
        match self {
            Node::Branch(branch) => Ok(branch),
            _ => Err(self),
        }
    }
}
