#![feature(trivial_bounds)]
#![feature(extern_prelude)]

#[macro_use]
extern crate slog;
extern crate slog_term;

/// Just give me a damn terminal logger
fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

pub mod srmap {
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::char;
    use std::borrow::Borrow;
    use std::sync::{Arc, RwLock};
    use std::marker::PhantomData;

    // SRMap inner structure
    #[derive(Clone, Debug)]
    pub struct SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash,
    {
        pub g_map: HashMap<K, Vec<V>>, // Global map
        pub meta: M,
        log: slog::Logger,
    }

    impl<K, V, M> SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug,
    {

        pub fn new(init_m: M) -> SRMap<K, V, M> {
            let logger = super::logger_pls();
            SRMap {
                g_map: HashMap::new(),
                meta: init_m,
                log: logger,
            }
        }

        pub fn key_statistics(&self, k: K) {
            let gmap = self.g_map.len();

            match self.g_map.get(&k) {
                Some(v) => trace!(self.log, "key: {:?}, len val: {:?}", k.clone(), v.len()),
                None => ()
            }

            info!(self.log, "SRMap total # of g_map records: {:?}", gmap);
        }

        pub fn statistics(&self) {
            let mut total_recs = 0;
            for (k, v) in &self.g_map {
                total_recs += v.len();
            }
            if total_recs % 1000 == 0 {
                info!(self.log, "SRMap total records across all keys: {:?}", total_recs);
            }
        }

        pub fn insert(&mut self, k: K, v: Vec<V>, uid: usize) {
            let mut insert = false;
            match self.g_map.get_mut(&k) {
                Some(vec) => {
                    for val in v.iter() {
                        vec.push(val.clone());
                    }
                },
                None => {
                    insert = true;
                }
            };
            if insert {
                self.g_map.insert(k.clone(), v);
            }
            self.statistics();
        }

        pub fn get(&self, k: &K, uid: usize) -> Option<Vec<V>> {
            //trace!(self.log, "SRMap: getting key {:?}, uid {:?}, gmap: {:?}, umap: {:?}", k.clone(), uid.clone(), self.g_map.clone(), self.u_map.clone());
            trace!(self.log, "SRMap: getting key {:?}, uid {:?}, gmap: {:?}", k.clone(), uid.clone(), self.g_map.clone());
            match self.g_map.get(&k) {
                Some(val) => Some(val.clone()),
                None => None
            }
        }

        pub fn remove(&mut self, k: K, uid: usize) {
            self.g_map.remove(&k);
        }

        pub fn add_user(&mut self, uid: usize) {

        }

        pub fn remove_user(&mut self, uid: usize) {

        }
    }

    use std::fmt::Debug;

    // SRMap WriteHandle wrapper structure
    #[derive(Debug, Clone)]
    pub struct WriteHandle<K, V, M = ()>
    where
        K: Eq + Hash + Clone + Debug,
        V: Clone + Eq + std::fmt::Debug + Hash,
   {
       handle: Arc<RwLock<SRMap<K, V, M>>>,
   }

   pub fn new_write<K, V, M>(
       lock: Arc<RwLock<SRMap<K, V, M>>>,
   ) -> WriteHandle<K, V, M>
   where
       K: Eq + Hash + Clone + std::fmt::Debug,
       V: Clone + Eq + std::fmt::Debug + Hash,
    {
        WriteHandle {
            handle: lock,
        }
    }

    impl<K, V, M> WriteHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash,
        M: Clone,
   {
       // Add the given value to the value-set of the given key.
       pub fn insert(&mut self, k: K, v: V, uid: usize) {
           let mut container = Vec::new();
           container.push(v);
           let mut w_handle = self.handle.write().unwrap();
           w_handle.insert(k.clone(), container, uid.clone());
       }

       // Replace the value-set of the given key with the given value.
       pub fn update(&mut self, k: K, v: V, uid: usize) {
           let mut container = Vec::new();
           container.push(v);
           let mut w_handle = self.handle.write().unwrap();
           w_handle.insert(k.clone(), container, uid.clone());
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

       pub fn refresh() {
           return
       }

       pub fn empty(&mut self, k: K, uid: usize) {
           let mut w_handle = self.handle.write().unwrap();
           w_handle.remove(k.clone(), uid.clone());
       }

       pub fn clear(&mut self, k: K, uid: usize) {
           let mut w_handle = self.handle.write().unwrap();
           w_handle.remove(k.clone(), uid.clone());
       }

       pub fn empty_at_index(&mut self, k: K, uid: usize) {
           let mut w_handle = self.handle.write().unwrap();
           w_handle.remove(k.clone(), uid.clone());
       }

       pub fn meta_get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<(Option<T>, M)>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           let r_handle = self.handle.read().unwrap();
           Some((r_handle.get(key, uid).map(move |v| then(&*v)), r_handle.meta.clone()))
       }

       pub fn is_empty(&self) -> bool {
           let r_handle = self.handle.read().unwrap();
           r_handle.g_map.is_empty()
       }

   }

   // SRMap ReadHandle wrapper structure
   #[derive(Debug, Clone)]
   pub struct ReadHandle<K, V, M = ()>
   where
       K: Eq + Hash + Clone + std::fmt::Debug,
       V: Clone + Eq + std::fmt::Debug + Hash,
    {
        pub(crate) inner: Arc<RwLock<SRMap<K, V, M>>>,
    }

    // ReadHandle constructor
    pub fn new_read<K, V, M>(store: Arc<RwLock<SRMap<K, V, M>>>) -> ReadHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash,
    {
        ReadHandle {
            inner: store,
        }
    }

    impl<K, V, M> ReadHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash,
        M: Clone,
    {
       /// Get the current meta value.
       pub fn meta(&self) -> Option<M> {
          self.with_handle(|inner| inner.meta.clone())
       }

       /// Applies a function to the values corresponding to the key, and returns the result.
       pub fn get_lock(&self) -> Arc<RwLock<SRMap<K, V, M>>>
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
       pub fn get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<T>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           let r_handle = self.inner.read().unwrap();
           r_handle.get(key, uid).map(move |v| then(&*v))
       }

       pub fn meta_get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<(Option<T>, M)>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           // trace!(self.log, "Wrapper func around inner map: trying to read: key {:?}, uid: {:?}", key.clone(), uid.clone());
           let r_handle = self.inner.read().unwrap();
           Some((r_handle.get(key, uid).map(move |v| then(&*v)), r_handle.meta.clone()))

       }

       fn with_handle<F, T>(&self, f: F) -> Option<T>
       where
          F: FnOnce(&SRMap<K, V, M>) -> T,
       {
           let r_handle = &*self.inner.read().unwrap();
           let res = Some(f(&r_handle));
           res
       }

       /// Read all values in the map, and transform them into a new collection.
       pub fn for_each<F>(&self, mut f: F)
       where
           F: FnMut(&K, &[V]),
       {
           self.with_handle(move |r_handle| {
            for (k, vs) in &r_handle.g_map {
                f(k, &vs[..])
            }
        });
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

   // Constructor for read/write handle tuple
   pub fn construct<K, V, M>(meta_init: M) -> (ReadHandle<K, V, M>, WriteHandle<K, V, M>)
   where
       K: Eq + Hash + Clone + std::fmt::Debug,
       V: Clone + Eq + std::fmt::Debug + Hash,
       M: Clone,
    {
        let locked_map = Arc::new(RwLock::new(SRMap::<K,V,M>::new(meta_init)));
        let r_handle = new_read(locked_map);
        let lock = r_handle.get_lock();
        let w_handle = new_write(lock);
        (r_handle, w_handle)
    }
}
