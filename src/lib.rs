#![feature(trivial_bounds)]
#![feature(extern_prelude)]
#![feature(test)]

#[macro_use]
extern crate slog;
extern crate slog_term;

extern crate test;

use test::Bencher;

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

    // SRMap inner structure
    #[derive(Clone, Debug)]
    pub struct SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash,
    {
        pub g_map: HashMap<K, Vec<V>>, // Global map
        g_records: usize,
        pub b_map: HashMap<(K, usize), Vec<bool>>, // Auxiliary bit map for global map
        pub u_map: HashMap<(String, K), Vec<V>>, // Universe specific map (used only when K,V conflict with g_map)
        pub id_store: HashMap<usize, usize>,
        largest: usize,
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
                g_records: 0,
                b_map: HashMap::new(),
                u_map: HashMap::new(),
                id_store: HashMap::new(),
                largest: 0 as usize,
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
            if self.g_records % 1000 == 0 {
                debug!(self.log, "SRMap total records across all keys: {:?}", self.g_records);
            }
        }

        pub fn insert(&mut self, k: K, v: Vec<V>, uid: usize) {
            // check if record is in the global map
            if self.g_map.contains_key(&k) {
                match self.g_map.get_mut(&k) {
                    Some(val) => {
                        let mut existing_values = HashMap::new();
                        let mut ind = 0 as usize;
                        for v_ in val.iter() {
                            existing_values.insert(v_.clone(), ind.clone());
                            ind = ind + 1;
                        }

                        // Append record to key's vec if uid = 0 (global)
                        if uid.clone() == 0 as usize {
                            for value in v.iter() {
                                if !existing_values.contains_key(&value.clone()) {
                                    // println!("Adding k: {:?} v: {:?} to global map ...", k.clone(), value.clone());
                                    val.push(value.clone());
                                    self.g_records += 1;

                                    let mut bit_map = Vec::new();
                                    let user_index = self.id_store.entry(uid).or_insert(0);
                                    for x in 0 .. self.largest + 1 {
                                        if x != *user_index {
                                            bit_map.push(false);
                                        } else {
                                            bit_map.push(true);
                                        }
                                    }
                                    self.b_map.insert((k.clone(), 0 as usize), bit_map);
                                } else {
                                    // println!("SRMap: Record already exists for this key.");
                                }
                            }
                        }

                        // For each new record inserted, check to see if the record is
                        // in the global map.
                        for value in v.iter() {
                            // If it is, update its bitmap.
                            if existing_values.contains_key(&value) {
                                let b_map_ind = existing_values.get(&value).unwrap();
                                let b_map_key = (k.clone(), *b_map_ind);
                                match self.b_map.get_mut(&b_map_key) {
                                    Some(mut bitmap) => {
                                        match self.id_store.get(&uid) {
                                            Some(&id) => {
                                                bitmap[id] = true;
                                            },
                                            None => {}
                                        }
                                    },
                                    None => {}
                                };
                            // If it's not, add it to vec in the user_specific map.
                            } else {
                                // Set up u_map key
                                let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
                                let key = (uid_str, k.clone());

                                // Add to an existing vec or create a new one
                                let mut insert = false;
                                match self.u_map.get_mut(&key) {
                                    Some(values) => {
                                        for value in v.iter() {
                                            values.push(value.clone());
                                        }
                                    },
                                    None => {
                                        insert = true;
                                    }
                                };
                                if insert {
                                    self.u_map.insert(key.clone(), v.clone());
                                }
                            }
                        }
                    },
                    None => {}
                }
            } else {
                let user_index = self.id_store.entry(uid).or_insert(0);
                self.g_map.insert(k.clone(), v.clone());
                self.g_records += v.len();
                let mut ind = 0 as usize;
                for value in v.iter() {
                    let mut bit_map = Vec::new();
                    for x in 0 .. self.largest + 1 {
                        if x != *user_index {
                            bit_map.push(false);
                        } else {
                            bit_map.push(true);
                        }
                    }
                    self.b_map.insert((k.clone(), ind.clone()), bit_map);
                    ind = ind + 1;
                }
            }
            self.statistics();
        }

        pub fn get(&self, k: &K, uid: usize) -> Option<Vec<V>> {
            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
            let key = (uid_str, k.clone());

            let mut to_return = Vec::new();

            let mut internal_id = 0 as usize;
            match self.id_store.get(&uid) {
                Some(&id) => {
                    internal_id = id;
                },
                None => {}
            }

            match self.u_map.get(&key) {
                Some(val) => {
                    for v in val.iter(){
                        to_return.push(v.clone());
                    }
                },
                None => {

                }
            }

            match self.g_map.get(&k) {
                Some(val) => {
                    let mut ind = 0;
                    for v in val.iter() {
                        let b_map_key = (k.clone(), ind.clone());
                        ind = ind + 1;
                        match self.b_map.get(&b_map_key) {
                            Some(bitmap) => {
                                if bitmap[internal_id] {
                                    to_return.push(v.clone());
                                }
                            },
                            None => {}
                        }
                    }
                },
                None => {}
            }

            let cloned = to_return.clone();

            if to_return.len() == 0 {
                return None
            } else {
                return Some(cloned)
            }
        }

        pub fn remove(&mut self, k: K, uid: usize) {
            let mut internal_id = 0 as usize;

            match self.id_store.get(&uid) {
                Some(&id) => {
                    internal_id = id;
                },
                None => {}
            }

            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
            let key = (uid_str, k.clone());

            let mut remove_entirely = true;
            let mut hit_inner = false;

            if self.u_map.contains_key(&key) {
                self.u_map.remove(&key);
            }

            match self.g_map.get(&k) {
                Some(values) => {
                    for i in 0..values.len() {
                        let b_map_key = (k.clone(), i as usize);
                        match self.b_map.get_mut(&b_map_key) {
                            Some(bitmap) => {
                                bitmap[internal_id] = false;
                                hit_inner = true;

                                for pt in bitmap {
                                    if *pt {
                                        remove_entirely = false;
                                    }
                                }
                            },
                            None => {}
                        }
                    }
                },
                None => {}
            }

            if remove_entirely && hit_inner {
                let size = self.g_map.get(&k).unwrap().len();
                self.g_map.remove(&k);
                self.g_records -= size;
                for i in 0..size {
                    self.b_map.remove(&(k.clone(), i as usize));
                }
            }
        }

        pub fn add_user(&mut self, uid: usize) {
            self.largest = self.largest + 1;
            self.id_store.insert(uid.clone(), self.largest.clone());

            // add bitmap flag for this user in every global bitmap
            for (_, bmap) in self.b_map.iter_mut() {
                bmap.push(false);
            }
        }

        pub fn remove_user(&mut self, uid: usize) {
            let mut keys_to_del = Vec::new();

            // remove all u_map records for this user and revoke access from all global entries
            match self.id_store.get(&uid) {
                Some(&id) => {
                    for (k, bmap) in self.b_map.iter_mut() {
                        bmap[id] = false;

                        // do some cleanup: delete record if no users access it anymore
                        let mut delete_whole = true;
                        for flag in bmap.iter() {
                            if *flag {
                                delete_whole = false;
                            }
                        }
                        if delete_whole {
                            keys_to_del.push(k.clone());
                        }
                    }
                },
                None => {}
            }

            for k in &keys_to_del {
                // FIXME(malte): this should adapt self.g_records as needed
                self.g_map.remove(&k.0);
                self.b_map.remove(k);
            }

            // remove all umap keys that start with this id
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



// pub mod srmap {
//     use std::collections::HashMap;
//     use std::hash::Hash;
//     use std::char;
//     use std::borrow::Borrow;
//     use std::sync::{Arc, RwLock};
//     use std::marker::PhantomData;
//
//     // SRMap inner structure
//     #[derive(Clone)]
//     #[derive(Serialize, Deserialize, Debug)]
//     pub struct SRMap<K, V, M>
//     where
//         K: Eq + Hash + Clone + std::fmt::Debug,
//         V: Clone + Eq + std::fmt::Debug + Hash,
//     {
//         pub g_map: HashMap<K, Vec<V>>, // Global map
//         pub meta: M,
//     }
//
//     impl<K, V, M> SRMap<K, V, M>
//     where
//         K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
//         V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Hash,
//         M: serde::Serialize + serde::de::DeserializeOwned,
//     {
//
//         pub fn new(init_m: M) -> SRMap<K, V, M> {
//             SRMap {
//                 g_map: HashMap::new(),
//                 meta: init_m
//             }
//         }
//
//         pub fn key_statistics(&self, k: K) {
//             let gmap = self.g_map.len();
//
//             match self.g_map.get(&k) {
//                 Some(v) => println!("key: {:?}, len val: {:?}", k.clone(), v.len()),
//                 None => ()
//             }
//
//             println!("total # of g_map records: {:?}", gmap);
//         }
//
//         pub fn statistics(&self) {
//             let mut total_recs = 0;
//             for (k, v) in &self.g_map {
//                 total_recs += v.len();
//             }
//             if total_recs % 1000 == 0 {
//                 println!("Total records across all keys: {:?}", total_recs);
//             }
//         }
//
//         pub fn insert(&mut self, k: K, v: Vec<V>, uid: usize) {
//             let mut insert = false;
//             match self.g_map.get_mut(&k) {
//                 Some(vec) => {
//                     for val in v.iter() {
//                         vec.push(val.clone());
//                     }
//                 },
//                 None => {
//                     insert = true;
//                 }
//             };
//             if insert {
//                 self.g_map.insert(k.clone(), v);
//             }
//             self.statistics();
//         }
//
//         pub fn get(&self, k: &K, uid: usize) -> Option<Vec<V>> {
//             // println!("SRMap: getting key {:?}, uid {:?}, gmap: {:?}, umap: {:?}", k.clone(), uid.clone(), self.g_map.clone(), self.u_map.clone());
//             match self.g_map.get(&k) {
//                 Some(val) => Some(val.clone()),
//                 None => None
//             }
//         }
//
//         pub fn remove(&mut self, k: K, uid: usize) {
//             self.g_map.remove(&k);
//         }
//
//         pub fn add_user(&mut self, uid: usize) {
//
//         }
//
//         pub fn remove_user(&mut self, uid: usize) {
//
//         }
//     }
//
//     use std::fmt::Debug;
//
//     // SRMap WriteHandle wrapper structure
//     #[derive(Deserialize, Serialize, Debug, Clone)]
//     pub struct WriteHandle<K, V, M = ()>
//     where
//         K: Eq + Hash + Clone + Debug,
//         V: Clone + Eq + std::fmt::Debug + Hash,
//    {
//        handle: Arc<RwLock<SRMap<K, V, M>>>,
//    }
//
//    pub fn new_write<K, V, M>(
//        lock: Arc<RwLock<SRMap<K, V, M>>>,
//    ) -> WriteHandle<K, V, M>
//    where
//        K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
//        V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Hash,
//        M: serde::Serialize + serde::de::DeserializeOwned,
//     {
//         WriteHandle {
//             handle: lock,
//         }
//     }
//
//     impl<K, V, M> WriteHandle<K, V, M>
//     where
//         K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
//         V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Hash,
//         M: Clone + serde::Serialize + serde::de::DeserializeOwned,
//    {
//        // Add the given value to the value-set of the given key.
//        pub fn insert(&mut self, k: K, v: V, uid: usize) {
//            let mut container = Vec::new();
//            container.push(v);
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.insert(k.clone(), container, uid.clone());
//        }
//
//        // Replace the value-set of the given key with the given value.
//        pub fn update(&mut self, k: K, v: V, uid: usize) {
//            let mut container = Vec::new();
//            container.push(v);
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.insert(k.clone(), container, uid.clone());
//        }
//
//        // Remove the given value from the value-set of the given key.
//        pub fn remove(&mut self, k: K, uid: usize) {
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.remove(k.clone(), uid.clone());
//        }
//
//        pub fn add_user(&mut self, uid: usize) {
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.add_user(uid.clone());
//        }
//
//        pub fn remove_user(&mut self, uid: usize) {
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.remove_user(uid.clone());
//        }
//
//        pub fn refresh() {
//            return
//        }
//
//        pub fn empty(&mut self, k: K, uid: usize) {
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.remove(k.clone(), uid.clone());
//        }
//
//        pub fn clear(&mut self, k: K, uid: usize) {
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.remove(k.clone(), uid.clone());
//        }
//
//        pub fn empty_at_index(&mut self, k: K, uid: usize) {
//            let mut w_handle = self.handle.write().unwrap();
//            w_handle.remove(k.clone(), uid.clone());
//        }
//
//        pub fn meta_get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<(Option<T>, M)>
//        where
//            K: Hash + Eq,
//            F: FnOnce(&[V]) -> T,
//        {
//            let r_handle = self.handle.read().unwrap();
//            Some((r_handle.get(key, uid).map(move |v| then(&*v)), r_handle.meta.clone()))
//        }
//
//        pub fn is_empty(&self) -> bool {
//            let r_handle = self.handle.read().unwrap();
//            r_handle.g_map.is_empty()
//        }
//
//    }
//
//    // SRMap ReadHandle wrapper structure
//    #[derive(Serialize, Deserialize, Debug, Clone)]
//    pub struct ReadHandle<K, V, M = ()>
//    where
//        K: Eq + Hash + Clone + std::fmt::Debug,
//        V: Clone + Eq + std::fmt::Debug + Hash,
//     {
//         pub(crate) inner: Arc<RwLock<SRMap<K, V, M>>>,
//     }
//
//     // ReadHandle constructor
//     pub fn new_read<K, V, M>(store: Arc<RwLock<SRMap<K, V, M>>>) -> ReadHandle<K, V, M>
//     where
//         K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
//         V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Hash,
//         M: serde::Serialize + serde::de::DeserializeOwned,
//     {
//         ReadHandle {
//             inner: store,
//         }
//     }
//
//     impl<K, V, M> ReadHandle<K, V, M>
//     where
//         K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
//         V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Hash,
//         M: Clone + serde::Serialize + serde::de::DeserializeOwned,
//     {
//        /// Get the current meta value.
//        pub fn meta(&self) -> Option<M> {
//           self.with_handle(|inner| inner.meta.clone())
//        }
//
//        /// Applies a function to the values corresponding to the key, and returns the result.
//        pub fn get_lock(&self) -> Arc<RwLock<SRMap<K, V, M>>>
//        {
//            self.inner.clone() // TODO make sure this is valid! want to keep only one locked map
//        }
//
//        /// Returns the number of non-empty keys present in the map.
//        pub fn len(&self) -> usize {
//            let r_handle = self.inner.read().unwrap();
//            r_handle.g_map.len()
//        }
//
//        /// Returns true if the map contains no elements.
//        pub fn is_empty(&self) -> bool {
//            let r_handle = self.inner.read().unwrap();
//            r_handle.g_map.is_empty()
//        }
//
//        /// Applies a function to the values corresponding to the key, and returns the result.
//        pub fn get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<T>
//        where
//            K: Hash + Eq,
//            F: FnOnce(&[V]) -> T,
//        {
//            let r_handle = self.inner.read().unwrap();
//            r_handle.get(key, uid).map(move |v| then(&*v))
//        }
//
//        pub fn meta_get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<(Option<T>, M)>
//        where
//            K: Hash + Eq,
//            F: FnOnce(&[V]) -> T,
//        {
//            // println!("Wrapper func around inner map: trying to read: key {:?}, uid: {:?}", key.clone(), uid.clone());
//            let r_handle = self.inner.read().unwrap();
//            Some((r_handle.get(key, uid).map(move |v| then(&*v)), r_handle.meta.clone()))
//
//        }
//
//        fn with_handle<F, T>(&self, f: F) -> Option<T>
//        where
//           F: FnOnce(&SRMap<K, V, M>) -> T,
//        {
//            let r_handle = &*self.inner.read().unwrap();
//            let res = Some(f(&r_handle));
//            res
//        }
//
//        /// Read all values in the map, and transform them into a new collection.
//        pub fn for_each<F>(&self, mut f: F)
//        where
//            F: FnMut(&K, &[V]),
//        {
//            self.with_handle(move |r_handle| {
//             for (k, vs) in &r_handle.g_map {
//                 f(k, &vs[..])
//             }
//         });
//        }
//
//        pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
//        where
//            K: Borrow<Q>,
//            Q: Hash + Eq,
//        {
//            let r_handle = self.inner.read().unwrap();
//            r_handle.g_map.contains_key(key)
//        }
//    }
//
//    // Constructor for read/write handle tuple
//    pub fn construct<K, V, M>(meta_init: M) -> (ReadHandle<K, V, M>, WriteHandle<K, V, M>)
//    where
//        K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
//        V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Hash,
//        M: Clone + serde::Serialize + serde::de::DeserializeOwned,
//     {
//         let locked_map = Arc::new(RwLock::new(SRMap::<K,V,M>::new(meta_init)));
//         let r_handle = new_read(locked_map);
//         let lock = r_handle.get_lock();
//         let w_handle = new_write(lock);
//         (r_handle, w_handle)
//     }
// }

#[bench]
fn bench_insert_throughput(b: &mut Bencher) {
    let uid1: usize = 0 as usize;
    let uid2: usize = 1 as usize;

    let (_r, mut w) = srmap::construct::<String, String, Option<i32>>(None);

    // create two users
    w.add_user(uid1);
    w.add_user(uid2);

    let k = "x".to_string();
    let v = "x".to_string();

    b.iter(|| {
        w.insert(k.clone(), v.clone(), 0);
    });
}

#[bench]
fn bench_insert_multival(b: &mut Bencher) {
    let uid1: usize = 0 as usize;
    let uid2: usize = 1 as usize;

    let (_r, mut w) = srmap::construct::<String, String, Option<i32>>(None);

    // create two users
    w.add_user(uid1);
    w.add_user(uid2);

    let k = "x".to_string();

    let mut i = 0;
    b.iter(|| {
        w.insert(k.clone(), format!("v{}", i), 0);
        i += 1;
    });
}


#[bench]
fn bench_get_throughput(b: &mut Bencher) {
    let uid1: usize = 0 as usize;
    let uid2: usize = 1 as usize;

    let (r, mut w) = srmap::construct::<String, String, Option<i32>>(None);

    // create two users
    w.add_user(uid1);
    w.add_user(uid2);

    let k = "x".to_string();
    let v = "x".to_string();

    w.insert(k.clone(), v.clone(), uid1);

    b.iter(|| {
        r.get_and(&k, |_| false, uid1);
    });
}
