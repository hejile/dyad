use std::{cell::RefCell, rc::Rc};

pub struct DyadContext<C> {
    data: Rc<RefCell<C>>,
}

impl<C> DyadContext<C> {
    pub fn new(data: Rc<RefCell<C>>) -> Self {
        Self {
            data,
        }
    }
    pub fn with<R>(&self, f: impl FnOnce(&C) -> R) -> R {
        f(&*self.data.borrow())
    }
    pub fn with_mut<R>(&self, f: impl FnOnce(&mut C) -> R) -> R {
        f(&mut *self.data.borrow_mut())
    }
}

impl<C> Clone for DyadContext<C> {
    fn clone(&self) -> Self {
        Self { data: self.data.clone() }
    }
}

struct ContextMetadata {
    
}

struct ContextHandle<C> {
    data: Rc<RefCell<C>>,

}