use std::{
    cell::RefCell,
    ops::{Deref, DerefMut},
};

pub struct SerialCell<T>(pub RefCell<T>);

unsafe impl<T> Send for SerialCell<T> {}

unsafe impl<T> Sync for SerialCell<T> {}

impl<T> Deref for SerialCell<T> {
    type Target = RefCell<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for SerialCell<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> SerialCell<T> {
    pub fn new(value: T) -> Self {
        Self(RefCell::new(value))
    }
}
