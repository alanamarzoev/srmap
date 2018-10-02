use std::borrow::Borrow;
use std::hash::{Hash};
use srmap::SRMap;
use std::sync::{Arc, RwLock};

/// A handle that may be used to read from the SRMap.
pub struct ReadHandle<K, V>
where
    K: Eq + Hash + std::fmt::Debug + Clone,
    std::string::String: std::convert::From<K>,
    V: Eq + Clone,
{
    pub(crate) inner: Arc<RwLock<SRMap<K, V>>>,
}


impl<K, V> Clone for ReadHandle<K, V>
where
    K: Eq + Hash + std::fmt::Debug + Clone,
    std::string::String: std::convert::From<K>,
    V: Eq + Clone,
{
    fn clone(&self) -> Self {
        ReadHandle {
            inner: self.inner.clone()
        }
    }
}

pub fn new<K, V>(store: Arc<RwLock<SRMap<K, V>>>) -> ReadHandle<K, V>
where
    K: Eq + Hash + std::fmt::Debug + Clone,
    std::string::String: std::convert::From<K>,
    V: Eq + Clone,
{
    ReadHandle {
        inner: store,
    }
}

impl<K, V> ReadHandle<K, V>
where
    K: Eq + Hash + std::fmt::Debug + Clone,
    std::string::String: std::convert::From<K>,
    V: Eq + Clone,
{
    pub fn get_lock(&self) -> Arc<RwLock<SRMap<K,V>>>
    {
        self.inner.clone() // TODO make sure this is valid! want to keep only one locked map
    }

    /// Returns the number of non-empty keys present in the map.
    pub fn len(&self) -> usize {
        let r_handle = self.inner.read().unwrap();
        r_handle.g_map.len()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        let r_handle = self.inner.read().unwrap();
        r_handle.g_map.is_empty()
    }

    /// Applies a function to the values corresponding to the key, and returns the result.
    pub fn get_and<F, T>(&self, key: K, then: F, uid: usize) -> Option<T>
    where
        F: FnOnce(&V) -> T,
    {
        let r_handle = self.inner.read().unwrap();
        // r_handle.what;
        match r_handle.get(key, uid) {
            Some(res) => Some(then(&res)),
            None => None
        }
    }

    /// Applies a function to the values corresponding to the key, and returns the result.
    pub fn get(&self, key: K, uid: usize) -> Option<V>
    {
        let r_handle = self.inner.read().unwrap();
        // r_handle.what;
        r_handle.get(key, uid)

    }

    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let r_handle = self.inner.read().unwrap();
        r_handle.g_map.contains_key(key)
    }
}



    // Read all values in the map, and transform them into a new collection.
    //
    // Be careful with this function! While the iteration is ongoing, any writer that tries to
    // refresh will block waiting on this reader to finish.
    // pub fn for_each<F>(&self, mut f: F)
    // where
    //     F: FnMut(&K, &[V]),
    // {
    //     self.with_handle(move |inner| {
    //         for (k, vs) in &inner.g_map {
    //             f(k, &vs[..])
    //         }
    //     });
    // }

    // Read all values in the map, and transform them into a new collection.
    // pub fn map_into<Map, Collector, Target>(&self, mut f: Map) -> Collector
    // where
    //     Map: FnMut(&K, &[V]) -> Target,
    //     Collector: FromIterator<Target>,
    // {
    //     self.with_handle(move |inner| {
    //         Collector::from_iter(inner.g_map.iter().map(|(k, vs)| f(k, &vs[..])))
    //     }).unwrap_or(Collector::from_iter(iter::empty()))
    // }
