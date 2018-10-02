#![feature(trivial_bounds)]
#![feature(extern_prelude)]

mod write;
mod read;

pub mod srmap {
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::char;

    use read::ReadHandle;
    use write::WriteHandle;

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

    pub fn construct<K, V>() -> (ReadHandle<K, V>, WriteHandle<K, V>)
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Eq + Clone,
        std::string::String: std::convert::From<K>,
    {
        use read;
        use write;
        let locked_map = Arc::new(RwLock::new(SRMap::<K,V>::new()));
        let r_handle = read::new(locked_map);
        let w_handle = write::new(r_handle.get_lock());
        (r_handle, w_handle)
    }
}
