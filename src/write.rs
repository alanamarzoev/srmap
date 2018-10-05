use std::hash::Hash;
use srmap::SRMap;
use std::sync::{Arc, RwLock};

pub struct WriteHandle<K, V>
where
    K: Eq + Hash + std::fmt::Debug + Clone,
    std::string::String: std::convert::From<K>,
    V: Eq + Clone,
{
    handle: Arc<RwLock<SRMap<K, V>>>,
}

pub(crate) fn new<K, V>(
    lock: Arc<RwLock<SRMap<K, V>>>,
) -> WriteHandle<K, V>
where
    K: Eq + Hash + std::fmt::Debug + Clone,
    std::string::String: std::convert::From<K>,
    V: Eq + Clone,
{
    WriteHandle {
        handle: lock,
    }
}

impl<K, V> WriteHandle<K, V>
where
    K: Eq + Hash + std::fmt::Debug + Clone,
    std::string::String: std::convert::From<K>,
    V: Eq + Clone,
{
    // Add the given value to the value-set of the given key.
    pub fn insert(&mut self, k: K, v: V, uid: usize) {
        let mut w_handle = self.handle.write().unwrap();
        w_handle.insert(k.clone(), v.clone(), uid.clone());
    }

    // Replace the value-set of the given key with the given value.
    pub fn update(&mut self, k: K, v: V, uid: usize) {
        let mut w_handle = self.handle.write().unwrap();
        w_handle.insert(k.clone(), v.clone(), uid.clone());
    }

    // Remove the given value from the value-set of the given key.
    pub fn remove(&mut self, k: K, uid: usize) {
        let mut w_handle = self.handle.write().unwrap();
        w_handle.remove(k.clone(), uid.clone());
    }

    pub fn add_user(&mut self, uid: usize) {
        let mut w_handle = self.handle.write().unwrap();
        w_handle.add_user(uid.clone());
    }

    pub fn remove_user(&mut self, uid: usize) {
        let mut w_handle = self.handle.write().unwrap();
        w_handle.remove_user(uid.clone());
    }

}

// impl<K, V> Extend<(K, V)> for WriteHandle<K, V>
// where
//     K: Eq + Hash + Clone,
//     V: Eq + ShallowCopy,
// {
//     fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
//         for (k, v) in iter {
//             self.insert(k, v);
//         }
//     }
// }
//
use std::ops::Deref;
impl<K, V> Deref for WriteHandle<K, V>
where
    K: Eq + Hash + Clone,
    V: Eq + ShallowCopy,
{
    type Target = ReadHandle<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
