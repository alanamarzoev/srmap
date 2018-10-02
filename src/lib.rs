#![feature(trivial_bounds)]
#![feature(extern_prelude)]


pub mod srmap {
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::char;
    use std::borrow::Borrow;
    use std::sync::{Arc, RwLock};

    #[derive(Clone)]
    #[derive(Debug)]
    pub struct SRMap<K, V>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        std::string::String: std::convert::From<K>,
        V: std::cmp::PartialEq + Clone + Eq,
    {
        pub g_map: HashMap<K, V>, // Global map
        pub b_map: HashMap<K, Vec<bool>>, // Auxiliary bit map for global map
        pub u_map: HashMap<String, V>, // Universe specific map (used only when K,V conflict with g_map)
        pub id_store: HashMap<usize, usize>,
        largest: i32
    }

    impl<K, V> SRMap<K, V>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        std::string::String: std::convert::From<K>,
        V: std::cmp::PartialEq + Clone + Eq,
    {

        pub fn new() -> SRMap<K, V> {
            SRMap{
                g_map: HashMap::new(),
                b_map: HashMap::new(),
                u_map: HashMap::new(),
                id_store: HashMap::new(),
                largest: -1
            }
        }

        pub fn insert(&mut self, k: K, v: V, uid: usize){
            println!("in insert!");
            // check if record is in the global map
            if self.g_map.contains_key(&k) {
                match self.g_map.get_mut(&k) {
                    Some(val) => {
                        // if it *is* in the global map, and the values match, update access for this user
                        if *val == v {
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
                        } else {
                        // if v is different, insert (k,v) into umap as ('uid:k',v)
                            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
                            let k_str: String = String::from(k).to_owned();
                            let u_key = format!("{}{}", uid_str, k_str);
                            self.u_map.insert(u_key.clone(), v.clone());
                        }
                    },
                    // add record to global map if it isn't already there
                    None => {}
                }
            } else {
                self.g_map.insert(k.clone(), v.clone());
                let mut bit_map = Vec::new();
                let user_index = self.id_store.entry(uid).or_insert(0);

                let largest = self.largest as usize;
                for x in 0..largest+1 {
                    if x != *user_index {
                        bit_map.push(false);
                    } else {
                        bit_map.push(true);
                    }
                }
                self.b_map.insert(k.clone(), bit_map);
            }
        }

        pub fn get(&self, k: K, uid: usize) -> Option<V> {
            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
            //let uid_str: String =  String::from(uid).to_owned();
            let k_str: String = String::from(k.clone()).to_owned();
            let first_check = format!("{}{}", uid_str, k_str);

            match self.u_map.get(&first_check) {
               Some(val) => {Some(val.clone())},
               _ => {match self.g_map.get(&k) {
                        Some(g_val) => {
                            match self.b_map.get(&k) {
                                Some(bitmap) => {
                                    match self.id_store.get(&uid) {
                                        Some(&id) => {
                                            let accessible = bitmap[id];
                                            if accessible {
                                                return Some(g_val.clone());
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
                            None
                        }
                     }
                 }
             }
        }

        pub fn remove(&mut self, k: K, uid: usize) {
            println!("in remove!");
            let uid_str = char::from_digit(uid as u32, 10).unwrap().to_string();
            let k_str: String = String::from(k.clone()).to_owned();
            let first_check = format!("{}{}", uid_str, k_str);
            let mut remove_entirely = true;
            let mut hit_inner = false;

            if self.u_map.contains_key(&first_check) {
                self.u_map.remove(&first_check);
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
            let largest_usize = self.largest as usize;
            self.id_store.insert(uid.clone(), largest_usize.clone());
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

    pub struct WriteHandle<K, V>
    where
        K: Eq + Hash + std::fmt::Debug + Clone,
        std::string::String: std::convert::From<K>,
        V: Eq + Clone,
    {
        handle: Arc<RwLock<SRMap<K, V>>>,
    }

    pub fn new_write<K, V>(
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

    pub fn new_read<K, V>(store: Arc<RwLock<SRMap<K, V>>>) -> ReadHandle<K, V>
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

    pub fn construct<K, V>() -> (ReadHandle<K, V>, WriteHandle<K, V>)
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Eq + Clone,
        std::string::String: std::convert::From<K>,
    {
        let locked_map = Arc::new(RwLock::new(SRMap::<K,V>::new()));
        let r_handle = new_read(locked_map);
        let lock = r_handle.get_lock();
        let w_handle = new_write(lock);
        //let gmap1 = lock.read().unwrap();
        (r_handle, w_handle)
    }


}
