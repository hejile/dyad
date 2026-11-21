use std::{cell::RefCell, rc::Rc};

pub struct DyadContext<C> {
    data: Rc<RefCell<C>>,
}

impl<C> DyadContext<C> {
    pub fn new(data: C) -> Self {
        Self {
            data: Rc::new(RefCell::new(data)),
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

impl<C: Default> Default for DyadContext<C> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}