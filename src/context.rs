use std::{cell::RefCell, ops::Deref, rc::Rc};

/// C is always accessed with immutable reference
/// use interior mutable type in C for mutation
#[derive(Debug)]
pub(crate) struct DyadContext<C> {
    data: Rc<C>,
}

#[derive(Debug)]
pub struct ParentContextHolder<P> {
    parent: Option<DyadContext<P>>,
}

impl<P> ParentContextHolder<P> {
    pub(crate) fn set_parent(&mut self, p: DyadContext<P>) {
        self.parent = Some(p)
    }
}

impl<P> Deref for ParentContextHolder<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &*self.parent.as_ref().expect("parent context not set")
    }
}

impl<P> Default for ParentContextHolder<P> {
    fn default() -> Self {
        Self { parent: Default::default() }
    }
}

pub trait ChildDyadContext {
    type Parent;

    fn parent_holder_mut(&mut self) -> &mut ParentContextHolder<Self::Parent>;
}

impl<C> DyadContext<C> {
    pub(crate) fn new(data: C) -> Self {
        Self {
            data: Rc::new(data),
        }
    }

    pub(crate) fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl<C> Deref for DyadContext<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &*self.data
    }
}

impl<C: Default> Default for DyadContext<C> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}
