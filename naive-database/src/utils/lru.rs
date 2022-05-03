#![allow(unused)]

use std::iter;

#[derive(Debug, Clone, Default)]
pub struct LruRecord {
    data: Vec<(usize, usize)>,
}

impl LruRecord {
    pub fn new(index_range: usize) -> Self {
        let data = iter::once((index_range, 1))
            .chain((1..index_range).map(|i| (i - 1, i + 1)))
            .chain(iter::once((index_range - 1, 0)))
            .collect();
        LruRecord { data }
    }

    fn link(&mut self, i: usize, j: usize) {
        if (i, j) == (0, 0) {
            return;
        }
        unsafe {
            let p = self.data.as_mut_ptr();
            (*p.add(i)).1 = j;
            (*p.add(j)).0 = i;
        }
    }

    fn remove(&mut self, i: usize) {
        let (prev, next) = self.data[i];
        self.link(prev, next);
        self.clear(i);
    }

    fn clear(&mut self, i: usize) {
        self.data[i] = (i, i);
    }

    pub fn access(&mut self, i: usize) {
        let i = i + 1;
        if i >= self.data.len() {
            return;
        }
        let (_, prev_head) = self.data[0];
        self.remove(i);
        self.link(0, i);
        self.link(i, prev_head);
    }

    pub fn find_furthest(&self) -> usize {
        self.data[0].0 - 1
    }
}

#[cfg(test)]
mod tests {
    use super::LruRecord;

    #[test]
    fn simple_lru_test() {
        let mut lru = LruRecord::new(3);
        lru.access(1);
        lru.access(0);
        assert_eq!(lru.find_furthest(), 2);
        lru.access(2);
        assert_eq!(lru.find_furthest(), 1);
        lru.access(1);
        assert_eq!(lru.find_furthest(), 0);
        lru.access(1);
        assert_eq!(lru.find_furthest(), 0);
        lru.access(0);
        assert_eq!(lru.find_furthest(), 2);
    }

    #[test]
    fn simple_lru_test_2() {
        let mut lru = LruRecord::new(3);
        assert_eq!(lru.find_furthest(), 2);
        lru.access(2);
        assert_eq!(lru.find_furthest(), 1);
        lru.access(0);
        assert_eq!(lru.find_furthest(), 1);
        lru.access(2);
        assert_eq!(lru.find_furthest(), 1);
        lru.access(1);
        assert_eq!(lru.find_furthest(), 0);
    }
}
