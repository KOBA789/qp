use std::ops::{Deref, DerefMut};

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use super::branch::Branch;
use super::leaf::Leaf;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum NodeType {
    Leaf = 1,
    Branch = 2,
}

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    node_type: u8,
    _pad: [u8; 7],
}

impl Header {
    fn node_type(&self) -> NodeType {
        if self.node_type == NodeType::Leaf as u8 {
            return NodeType::Leaf;
        }
        if self.node_type == NodeType::Branch as u8 {
            return NodeType::Branch;
        }
        unreachable!()
    }

    fn set_node_type(&mut self, node_type: NodeType) {
        self.node_type = node_type as u8;
    }
}

pub struct NodePage<B> {
    header: LayoutVerified<B, Header>,
    body: B,
}

impl<B: ByteSlice> NodePage<B> {
    pub fn new(bytes: B) -> Option<Self> {
        let (header, body) = LayoutVerified::new_from_prefix(bytes)?;
        Some(Self { header, body })
    }

    pub fn node(&self) -> Node<&[u8]> {
        match self.header.node_type() {
            NodeType::Leaf => Node::Leaf(Leaf::new(self.body.deref()).unwrap()),
            NodeType::Branch => Node::Branch(Branch::new(self.body.deref()).unwrap()),
        }
    }
}

impl<B: ByteSliceMut> NodePage<B> {
    pub fn initialize_as_leaf(&mut self) -> Leaf<&mut [u8]> {
        self.header.set_node_type(NodeType::Leaf);
        Leaf::new(self.body.deref_mut()).unwrap()
    }

    pub fn initialize_as_branch(&mut self) -> Branch<&mut [u8]> {
        self.header.set_node_type(NodeType::Branch);
        Branch::new(self.body.deref_mut()).unwrap()
    }

    pub fn node_mut(&mut self) -> Node<&mut [u8]> {
        match self.header.node_type() {
            NodeType::Leaf => Node::Leaf(Leaf::new(self.body.deref_mut()).unwrap()),
            NodeType::Branch => Node::Branch(Branch::new(self.body.deref_mut()).unwrap()),
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
