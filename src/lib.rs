#![feature(trivial_bounds)]
#![feature(extern_prelude)]

#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;

pub mod srmap {
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::char;
    use std::borrow::Borrow;
    use std::sync::{Arc, RwLock};

    // SRMap inner structure
    #[derive(Clone)]
    #[derive(Serialize, Deserialize, Debug)]
    pub struct SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug,
    {
        pub g_map: HashMap<K, Vec<V>>, // Global map
        pub b_map: HashMap<K, Vec<bool>>, // Auxiliary bit map for global map
        pub u_map: HashMap<(String, K), Vec<V>>, // Universe specific map (used only when K,V conflict with g_map)
        pub id_store: HashMap<usize, usize>,
        largest: usize,
        pub meta: M,
    }

    impl<K, V, M> SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
        V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
        M: serde::Serialize + serde::de::DeserializeOwned,
    {

        pub fn new(init_m: M) -> SRMap<K, V, M> {
            SRMap {
                g_map: HashMap::new(),
                b_map: HashMap::new(),
                u_map: HashMap::new(),
                id_store: HashMap::new(),
                largest: 0,
                meta: init_m
            }
        }

        pub fn statistics(&self) {
            let gmap = self.g_map.len();
            let umap = self.u_map.len();
            println!("SRMAP STATS: # g_map records: {:?}, # u_map records: {:?}, TOTAL #: {:?}", gmap.clone(), umap.clone(), gmap + umap);
        }

        pub fn insert(&mut self, k: K, v: Vec<V>, uid: usize) {
            println!("INSERTING key k: {:?}, v: {:?}, uid: {:?}", k.clone(), v.clone(), uid.clone()); 
            // check if record is in the global map
            if self.g_map.contains_key(&k) {
                match self.g_map.get_mut(&k) {
                    Some(val) => {
                        // if it *is* in the global map, and the values match, update access for this user
                        if *val == v {
                            println!("updating access to record k={:?} v={:?}", k.clone(), v.clone());
                            // update flag in global bit map for this user
                            match self.b_map.get_mut(&k) {
                                Some(mut bitmap) => {
                                    match self.id_store.get(&uid) {
                                        Some(&id) => {
                                            bitmap[id] = true;
                                        },
                                        None => {}
                                    }
                                },
                                None => {}
                            }
                        }
                        else {
                            // if v is different, insert (k,v) into umap as ('uid:k',v)
                            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
                            let key = (uid_str, k.clone());
                            println!("inserting new record k={:?}, v={:?} (conflict in global map)", k.clone(), v.clone());
                            println!("umap prior to add: {:?}", self.u_map.clone());
                            self.u_map.insert(key, v.clone());
                            println!("umap after add: {:?}", self.u_map.clone());
                        }
                    },
                    // add record to global map if it isn't already there
                    None => {}
                }
            } else {
                println!("inserting new record k={:?}, v={:?} (first time added to global map)", k.clone(), v.clone());
                println!("gmap prior to add: {:?}", self.g_map.clone());
                self.g_map.insert(k.clone(), v.clone());
                println!("gmap after add: {:?}", self.g_map.clone());
                let mut bit_map = Vec::new();
                let user_index = self.id_store.entry(uid).or_insert(0);

                for x in 0 .. self.largest + 1 {
                    if x != *user_index {
                        bit_map.push(false);
                    } else {
                        bit_map.push(true);
                    }
                }
                self.b_map.insert(k.clone(), bit_map);
            }
            self.statistics();
        }

        pub fn get(&self, k: &K, uid: usize) -> Option<&Vec<V>> {
            println!("GETTING key {:?} uid {:?}, gmap: {:?}, umap: {:?}", k.clone(), uid.clone(), self.g_map.clone(), self.u_map.clone());
            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
            let key = (uid_str, k.clone());
            match self.u_map.get(&key) {
               Some(val) => {Some(&val)},
               _ => {match self.g_map.get(&k) {
                        Some(g_val) => {
                            match self.b_map.get(&k) {
                                Some(bitmap) => {
                                    match self.id_store.get(&uid) {
                                        Some(&id) => {
                                            let accessible = bitmap[id];
                                            if accessible {
                                                let return_val = Some(g_val);
                                                return return_val;
                                            }
                                            else {
                                                return None;
                                            }
                                        },
                                        None => {None}
                                    }
                                },
                                None => {
                                    None
                                }
                            }
                        },
                        _ => {
                            println!("RECORD NOT FOUND!");
                            None
                        }
                     }
                 }
             }
        }

        pub fn remove(&mut self, k: K, uid: usize) {
            println!("removing k: {:?} for uid: {:?}", k.clone(), uid.clone());

            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
            let key = (uid_str, k.clone());

            let mut remove_entirely = true;
            let mut hit_inner = false;

            if self.u_map.contains_key(&key) {
                self.u_map.remove(&key);
            }

            if self.g_map.contains_key(&k){
                match self.b_map.get_mut(&k){
                    Some(bitmap) => {
                        match self.id_store.get(&uid) {
                            Some(&id) => {
                                println!("here...");
                                bitmap[id] = false;
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
                    None => {}
                }
            }

            if remove_entirely && hit_inner {
                self.g_map.remove(&k);
                self.b_map.remove(&k);
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
                self.g_map.remove(k);
                self.b_map.remove(k);
            }

            // remove all umap keys that start with this id
        }
    }

    use std::fmt::Debug;

    // SRMap WriteHandle wrapper structure
    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct WriteHandle<K, V, M = ()>
    where
        K: Eq + Hash + Clone + Debug,
        V: Clone + Eq + std::fmt::Debug,
   {
       handle: Arc<RwLock<SRMap<K, V, M>>>,
   }

   pub fn new_write<K, V, M>(
       lock: Arc<RwLock<SRMap<K, V, M>>>,
   ) -> WriteHandle<K, V, M>
   where
       K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
       V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
       M: serde::Serialize + serde::de::DeserializeOwned,
    {
        WriteHandle {
            handle: lock,
        }
    }

    impl<K, V, M> WriteHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
        V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
        M: Clone + serde::Serialize + serde::de::DeserializeOwned,
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
           Some((r_handle.get(key, uid).map(move |v| then(&**v)), r_handle.meta.clone()))
       }

       pub fn is_empty(&self) -> bool {
           let r_handle = self.handle.read().unwrap();
           r_handle.g_map.is_empty()
       }

   }

   // SRMap ReadHandle wrapper structure
   #[derive(Serialize, Deserialize, Debug, Clone)]
   pub struct ReadHandle<K, V, M = ()>
   where
       K: Eq + Hash + Clone + std::fmt::Debug,
       V: Clone + Eq + std::fmt::Debug
    {
        pub(crate) inner: Arc<RwLock<SRMap<K, V, M>>>,
    }

    // ReadHandle constructor
    pub fn new_read<K, V, M>(store: Arc<RwLock<SRMap<K, V, M>>>) -> ReadHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
        V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
        M: serde::Serialize + serde::de::DeserializeOwned,
    {
        ReadHandle {
            inner: store,
        }
    }

    impl<K, V, M> ReadHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
        V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
        M: Clone + serde::Serialize + serde::de::DeserializeOwned,
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
           r_handle.get(key, uid).map(move |v| then(&**v))
       }

       pub fn meta_get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<(Option<T>, M)>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           println!("Wrapper func around inner map: trying to read: key {:?}, uid: {:?}", key.clone(), uid.clone());
           let r_handle = self.inner.read().unwrap();
           Some((r_handle.get(key, uid).map(move |v| then(&**v)), r_handle.meta.clone()))

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
       K: Eq + Hash + Clone + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
       V: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
       M: Clone + serde::Serialize + serde::de::DeserializeOwned,
    {
        let locked_map = Arc::new(RwLock::new(SRMap::<K,V,M>::new(meta_init)));
        let r_handle = new_read(locked_map);
        let lock = r_handle.get_lock();
        let w_handle = new_write(lock);
        (r_handle, w_handle)
    }
}
