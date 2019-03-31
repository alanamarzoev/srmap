/// Just give me a damn terminal logger
// fn logger_pls() -> slog::Logger {
//     use slog::Drain;
//     use slog::Logger;
//     use slog_term::term_full;
//     use std::sync::Mutex;
//     Logger::root(Mutex::new(term_full()).fuse(), o!())
// }

pub mod srmap {
    use evmap;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::sync::Mutex;
    use std::sync::{Arc, RwLock};
    use bit_vec::BitVec;


    pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};

    // Bitmap update functions
    pub fn update_access(bitmap: &mut BitVec, uid: usize, add: bool) {
        let bmap_len = bitmap.len();
        if bmap_len <= uid {
            let num_new_elements = uid - (bmap_len - 1);
            for _el in 0..num_new_elements {
                bitmap.push(false);
            }
        }
        if add {
            bitmap.set(uid, true);
        } else {
            bitmap.set(uid, false);
        }
    }


    pub fn get_access(bitmap: &BitVec, uid: usize) -> bool {
        if uid == 0 {
            return true;
        }
        let bmap_len = bitmap.len();
        if bmap_len <= uid {
            return false;
        } else {
            return bitmap.get(uid).unwrap();
        }
    }

    // SRMap inner structure
    pub struct SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {
        pub g_map_r: evmap::ReadHandle<K, V>,
        pub b_map_r: evmap::ReadHandle<(K, V), Vec<BitVec>>,
        pub global_w: Arc<
            Mutex<(
                evmap::WriteHandle<K, V>,
                evmap::WriteHandle<(K, V), Vec<BitVec>>,
            )>,
        >,
        pub id_store: Arc<RwLock<HashMap<usize, usize>>>,
        pub meta: M,
        largest: Arc<RwLock<usize>>,
        g_records: usize,
        // log: slog::Logger,
    }

    impl<K, V, M> Clone for SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
        M: Clone,
    {
        fn clone(&self) -> Self {
            // let logger = super::logger_pls();
            SRMap {
                g_map_r: self.g_map_r.clone(),
                b_map_r: self.b_map_r.clone(),
                global_w: self.global_w.clone(),
                id_store: self.id_store.clone(),
                largest: self.largest.clone(),
                meta: self.meta.clone(),
                g_records: self.g_records.clone(),
                // log: logger,
            }
        }
    }

    impl<K, V, M> SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
        M: Clone,
    {
        pub fn new(init_m: M) -> SRMap<K, V, M> {
            // let logger = super::logger_pls();
            let (g_map_r, g_map_w) = evmap::new();
            let (b_map_r, b_map_w) = evmap::new();
            SRMap {
                g_map_r: g_map_r,
                global_w: Arc::new(Mutex::new((g_map_w, b_map_w))),
                b_map_r: b_map_r,
                id_store: Arc::new(RwLock::new(HashMap::new())),
                meta: init_m,
                g_records: 0,
                largest: Arc::new(RwLock::new(0 as usize)),
                // log: logger,
            }
        }

        pub fn g_map_size(&self) -> usize {
            let mut gm_vec = Vec::new();
            self.g_map_r.for_each(|_, _| gm_vec.push(1));
            gm_vec.len()
        }

        pub fn get_id(&self, uid: usize) -> Option<usize> {
            // println!("id store: {:?}", self.id_store.read().unwrap());
            match self.id_store.read().unwrap().get(&uid) {
                Some(&id) => Some(id.clone()),
                None => None,
            }
        }

        pub fn refresh(&mut self) {
            let (ref mut g_map_w, ref mut b_map_w) = *self.global_w.lock().unwrap();
            g_map_w.refresh();
            b_map_w.refresh();
        }

        pub fn insert(&mut self, k: K, v: Vec<V>, uid: usize) -> bool {
            let (ref mut g_map_w, ref mut b_map_w) = *self.global_w.lock().unwrap();
            // global map insert.
            if uid == 0 as usize {
                for val in v.clone() {
                    self.g_records += 1;
                    g_map_w.insert(k.clone(), val.clone());
                    let mut outer = Vec::new();
                    let mut buffer = Vec::new();
                    let mut bit_map = BitVec::new();
                    bit_map.push(false);
                    b_map_w.get_and(&(k.clone(), val.clone()), |s| {
                        if s.len() > 0 {
                            outer.push(s[0].clone());
                        } else {
                            outer.push(Vec::new());
                        }
                    });
                    if outer.len() > 0 {
                        buffer = outer[0].to_vec();
                    }
                    buffer.push(bit_map);
                    b_map_w.update((k.clone(), val.clone()), buffer);
                }
                g_map_w.refresh();
                b_map_w.refresh();
                return true;
            } else {
                // if value exists in the global map, remove this user's name from restricted access list.
                // otherwise, add record to the user's umap.
                let mut res = false;
                self.g_map_r.get_and(&k.clone(), |vs| {
                    for val in &v {
                        let mut last_seen = 0;
                        let mut count = 0 as usize;
                        // attempting to find a match for this value in the global map
                        // _that this user does not yet have access to_. if this is successful,
                        // indicate that the value has been found, and update access.
                        // if not successful, insert into umap. repeat for all values.
                        for v in vs {
                            if *v == *val && count >= last_seen {
                                let mut found = false;
                                self.b_map_r.get_and(&(k.clone(), val.clone()), |s| {
                                    // if user doesn't yet have access to a record with a matching
                                    // value in the global map, then update this bitmap to grant
                                    // access. otherwise, add to user map.
                                    match get_access(&s[0][count], uid) {
                                        true => {
                                            last_seen = count;
                                            count = count + 1 as usize;
                                        }
                                        false => {
                                            found = true;
                                            let mut bmaps = s[0].clone();
                                            // println!("bmap before: {:?}", bmap[count]);
                                            update_access(&mut bmaps[count], uid, true);
                                            // println!("bmap after: {:?}", bmap[count]);
                                            let bmkey = (k.clone(), val.clone());
                                            b_map_w.update(bmkey.clone(), bmaps);
                                            res = true;
                                        }
                                    }
                                });
                                if found {
                                    break;
                                }
                            }
                        }
                    }
                    // b_map_w.refresh();
                });
                return res;
            }
        }


        pub fn get(&self, k: &K, uid: usize) -> Option<Vec<V>> {
            let mut res_list = Vec::new();
            let mut seen_so_far: HashMap<(K, V), i32> = HashMap::new();
            self.g_map_r.get_and(&k, |set| {
                let mut increment_seen_so_far = false;
                let mut past_count = 0;
                for v in set {
                    match seen_so_far.get(&(k.clone(), v.clone())) {
                        Some(count) => {
                            past_count = count.clone();
                            match self
                                .b_map_r
                                .get_and(&(k.clone(), v.clone()), |s| s[0].clone())
                            {
                                Some(bmap) => {
                                    if get_access(&bmap[count.clone() as usize], uid) {
                                        res_list.push(v.clone());
                                        increment_seen_so_far = true;
                                    }
                                }
                                None => {}
                            }
                        },
                        None => {
                            match self
                                .b_map_r
                                .get_and(&(k.clone(), v.clone()), |s| s[0].clone())
                            {
                                Some(bmap) => {
                                    if get_access(&bmap[0], uid) {
                                        res_list.push(v.clone());
                                        increment_seen_so_far = true;
                                    }
                                }
                                None => {}
                            }
                        }
                    }

                    seen_so_far.insert((k.clone(), v.clone()), past_count + 1);
                }
            });

            return Some(res_list);

        }

        pub fn remove(&mut self, k: &K, uid: usize) {
            let mut seen_so_far: HashMap<(K, V), i32> = HashMap::new();
            self.g_map_r.get_and(&k, |set| {
                for v in set.iter() {
                    let mut increment = false;
                    let mut old_count = 0;
                    let bm_key = &(k.clone(), v.clone());
                    let mut bmap = self.b_map_r.get_and(&bm_key.clone(), |s| s[0].clone());
                    match seen_so_far.get(bm_key) {
                        Some(count) => {
                            old_count = count.clone();
                            match bmap {
                                Some(mut bm) => {
                                    if bm.len() > (count.clone() as usize) {
                                        update_access(&mut bm[count.clone() as usize], uid, false);
                                        increment = true;
                                    }
                                }
                                None => {}
                            }
                        },
                        None => {
                            match bmap {
                                Some(mut bm) => {
                                    if bm.len() > 0 {
                                        update_access(&mut bm[0], uid, false);
                                        increment = true;
                                    }
                                }
                                None => {}
                            }
                        }
                    };

                    if increment {
                        seen_so_far.insert(bm_key.clone(), old_count + 1);
                    }
                }
            });
        }

        pub fn add_user(&mut self) -> usize {
            // capture new id
            let id = self.largest.read().unwrap().clone();

            // update largest so that next ID is one higher
            let mut largest = self.largest.write().unwrap();
            *largest += 1;

            return id; // return internal id
        }

        // Get all records that a given user has access to
        pub fn get_all(&self, uid: usize) -> Option<Vec<(K, V)>> {
            let mut buffer = Vec::new();

            self.g_map_r.for_each(|k, v| {
                for val in v {
                    buffer.push((k.clone(), val.clone()));
                }
            });

            println!("gmap_r len: {:?}", self.g_map_r.len());
            println!("ALL POSTS IN MAP: {:#?}", buffer);
            let mut to_return = Vec::new();

            for (k, val) in buffer.iter() {
                let bmkey = (k.clone(), val.clone());
                let mut bmap = self.b_map_r.get_and(&bmkey, |s| s[0].clone()).unwrap();
                if get_access(&bmap[0], uid) {
                    to_return.push(bmkey);
                }
            }

            return Some(to_return);
        }
    }

}
