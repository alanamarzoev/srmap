#![feature(trivial_bounds)]
#![feature(extern_prelude)]
#![feature(test)]

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate evmap;
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
    use std::collections::{HashMap, HashSet};
    use std::hash::Hash;
    use std::char;
    use std::borrow::Borrow;
    use std::sync::{Arc, RwLock};
    use evmap;
    use std::iter::FromIterator;
    use std::rc::Rc;
    use std::sync::Mutex;

    // SRMap inner structure
    #[derive(Clone)]
    pub struct SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {
        pub g_map_r: evmap::ReadHandle<K, V>,
        pub b_map_r: evmap::ReadHandle<(K, V), Vec<usize>>,
        pub global_w: Arc<
            Mutex<(
                evmap::WriteHandle<K, V>,
                evmap::WriteHandle<(K, V), Vec<usize>>,
            )>,
        >,
        pub u_map: Vec<Arc<RwLock<HashMap<K, Vec<V>>>>>,
        pub id_store: HashMap<usize, usize>,
        pub meta: M,
        largest: usize,
        g_records: usize,
        log: slog::Logger,
    }

    impl<K, V, M> SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
    {
        pub fn new(init_m: M) -> SRMap<K, V, M> {
            let logger = super::logger_pls();
            let (g_map_r, mut g_map_w) = evmap::new();
            let (b_map_r, mut b_map_w) = evmap::new();
            let mut u_map = Vec::new();
            u_map.push(Arc::new(RwLock::new(HashMap::new())));
            SRMap {
                g_map_r: g_map_r,
                global_w: Arc::new(Mutex::new((g_map_w, b_map_w))),
                b_map_r: b_map_r,
                u_map: u_map,
                id_store: HashMap::new(),
                meta: init_m,
                g_records: 0,
                largest: 0 as usize,
                log: logger,
            }
        }

        pub fn g_map_size(&self) -> usize {
            let mut gm_vec = Vec::new();
            self.g_map_r.for_each(|x, s| gm_vec.push(1));
            gm_vec.len()
        }

        pub fn get_id(&self, uid: usize) -> Option<usize> {
            match self.id_store.get(&uid) {
                Some(&id) => Some(id.clone()),
                None => None
            }
        }

        // Only the global universe writes to the global map.
        // Writes to user universes will first check to see if the record exists in
        // the global universe. If it does, a bit will be flipped to indicate access.
        // If it doesn't exist in the global universe, the record is added to the user
        // universe.
        pub fn insert(&mut self, k: K, v: Vec<V>, id: usize) {
            // If uid is 0, insert records into global universe

            let mut uid = self.get_id(id.clone());
            if uid == None {
                self.add_user(id.clone());
                uid = self.get_id(id.clone());
            }
            let uid = uid.unwrap();

            let (ref mut g_map_w, ref mut b_map_w) = *self.global_w.lock().unwrap();

            b_map_w.flush();

            if (uid == (0 as usize)) {
                // Add record to existing set of values if it exists, otherwise create a new set
                // let mut g_map_w = &mut self.g_map_w.lock().unwrap();
                // let mut b_map_w = &mut self.b_map_w.lock().unwrap();
                for val in v {
                    // println!("here5");

                    // Add (index, value) to global map
                    g_map_w.insert(k.clone(), val.clone());

                    // Create new bitmap for this value
                    let mut bit_map = Vec::new();
                    for x in 0..self.largest + 1 {
                        if x == 0 {
                            bit_map.push(1 as usize);
                        } else {
                            bit_map.push(0 as usize);
                        }
                    }
                    b_map_w.insert((k.clone(), val.clone()), bit_map);
                }
                g_map_w.refresh();
                b_map_w.refresh();

            } else {
                // If value exists in global map and isn't accessible -> flip a bit.
                // Otherwise, add the value to the user's map.
                let mut u_map = self.u_map[uid].write().unwrap();
                let mut added = false;
                let mut same_as_global = self.g_map_r.get_and(&k.clone(), |vs| {
                    for val in &v {
                        let mut last_seen = 0;
                        let mut count = 0 as usize;
                        let mut found = false;
                        let mut bmap : Vec<usize> = Vec::new();

                        for v in vs {
                            if *v == *val && count > last_seen && found == false {
                                self.b_map_r.get_and(&(k.clone(), val.clone()), |s| { if s[count][uid] == 0 { found = true;
                                                                                                              bmap = s[count].clone()}
                                                                                                             else { last_seen = count; }});
                                count = count + 1 as usize;
                            }
                        }

                        if found {
                            bmap[uid] = 1 as usize;
                            b_map_w.update((k.clone(), val.clone()), bmap.clone());
                            b_map_w.refresh();
                        } else {
                            match u_map.get_mut(&k){
                                Some(vec) => vec.push(val.clone()),
                                None => { let mut new_vec = Vec::new(); new_vec.push(val.clone()); }
                            }
                        }
                    }
                });

                // User insert. First check to see if the value exists in the global map.
                // If it does, update the bitmap. If it doesn't, add to the user's map.
                // let mut u_map_insert = false;

                //                let mut same_as_global = self.g_map_r.get_and(&k, |vs| {
                //                    for val in &v {
                //                        if vs.contains(val) {
                //                            // check bitmap
                //                        } else {
                //                        }
                //                });
                //                match value_set {
                //                    Some(set) => {
                //                        for val in &v {
                //                            match set.get(val) {
                //                                Some(value) => {
                //                                    // let mut b_map_w = &mut b_map_w.lock().unwrap();
                //                                    let bm_key = (k.clone(), value.clone());
                //                                    b_map_w.refresh();
                //                                    let mut bm =
                //                                        self.b_map_r.get_and(&bm_key, |s| s[0].clone()).unwrap();
                //                                    // println!("bmkey: {:?}", bm_key.clone());
                //                                    // println!("about to access bm {:?}, indexed by uid: {:?}", bm.clone(), uid.clone());
                //                                    bm[uid] = 1 as usize;
                //                                    b_map_w.update(bm_key, bm);
                //                                }
                //                                None => {
                //                                    u_map_insert = true;
                //                                }
                //                            }
                //                        }
                //                    }
                //                    None => {
                //                        u_map_insert = true;
                //                    }
                //                };

                // Insert into user map.
                // if u_map_insert {
                //     // println!("here6");
                //
                //     let mut v_set: HashSet<V> = v.iter().cloned().collect();
                //     // println!("Accessing umap of uid {:?}", uid.clone());
                //     let mut u_map = self.u_map[uid].write().unwrap();
                //     {
                //         let mut res_set = u_map.get(&k);
                //         if res_set != None {
                //             let mut res_set = res_set.unwrap();
                //             v_set = v_set.union(res_set).cloned().collect();
                //         }
                //     }
                //     u_map.insert(k.clone(), v_set);
                //
                // }
            }
        }


        pub fn get(&self, k: &K, id: usize) -> Option<Vec<V>> { //TODO optimize this!! will prob be slow
            let mut id = self.get_id(id.clone());
            let mut uid = 0;
            if id == None {
                return None
            } else {
                uid = id.unwrap();
            }

            let mut u_map = self.u_map[uid].write().unwrap();

            let mut v_vec = u_map.get_mut(k);
            let mut res_list : Vec<V>;
            if v_vec != None {
                res_list = v_vec.unwrap().clone();
            } else {
                res_list = Vec::new();
            }

            self.g_map_r.get_and(&k, |set| {
                for v in set {
                    let access = self
                        .b_map_r
                        .get_and(&(k.clone(), v.clone()), |s| s[0].clone())
                        .unwrap()[uid];
                    if access == 1 as usize {
                        res_list.push(v.clone());
                    }
                }
            });

            let mut to_return = Vec::new();
            for x in res_list.iter() {
                to_return.push(x.clone());
            }
            if to_return.len() > 0 {
                return Some(to_return)
            } else {
                return None
            }
        }

        pub fn remove(&mut self, k: &K, id: usize) {
            let mut uid = self.get_id(id.clone());
            if uid == None {
                self.add_user(id.clone());
                uid = self.get_id(id.clone());
            }
            let uid = uid.unwrap();

            let mut u_map = self.u_map[uid].write().unwrap();
            u_map.remove(k);

            self.g_map_r.get_and(&k, |set| {
                for v in set.iter() {
                    let bm_key = &(k.clone(), v.clone());
                    let mut bmap = self.b_map_r.get_and(bm_key, |s| s[0].clone());
                    match bmap {
                        Some(mut bm) => {
                            bm[uid] = 0 as usize;
                        }
                        None => {}
                    }
                }
            });
        }

        pub fn add_user(&mut self, uid: usize) {
            // add to ID store
            if self.largest == 0 && uid == 0 {
                self.largest = 0;
            } else {
                self.largest = self.largest + 1;
            }
            // create user map
            let mut um = Arc::new(RwLock::new(HashMap::new()));
            self.u_map.push(um);

            // println!("Adding umap for user with uid: {:?} internal: {:?}, len of umap vec: {:?}", uid.clone(), self.largest.clone(), self.u_map.len());

            // add bitmap flag for this user in every global bitmap
            let mut new_bm = Vec::new();
            self.b_map_r.for_each(|k, v| { new_bm.push((k.clone(), v[0].clone())) });

            let (ref mut g_map_w, ref mut b_map_w) = *self.global_w.lock().unwrap();

            for y in new_bm.iter() {
                let mut kv = y.0.clone();
                let mut v = y.1.clone();
                v.push(0);
                // println!("updating bmaps: {:?} {:?}", kv.clone(), v.clone());

                b_map_w.insert(kv.clone(), v.clone());

            }
            b_map_w.flush();

            self.id_store.insert(uid.clone(), self.largest.clone());

        }

        // Get all records that a given user has access to
        pub fn get_all(&self, id: usize) -> Option<Vec<(K, V)>> {
            let mut id = self.get_id(id.clone());
            let mut uid = 0;
            if id == None {
                return None
            } else {
                uid = id.unwrap();
            }

            let mut u_map = self.u_map[uid].read().unwrap();
            let mut to_return : Vec<(K, V)> = Vec::new();

            for (k, v) in u_map.iter() {
                for val in v.iter() {
                    to_return.push((k.clone(), val.clone()));
                }
            }

            let mut buffer = Vec::new();
            self.g_map_r
                .for_each(|k, v| buffer.push((k.clone(), v[0].clone())));

            for (k, val) in buffer.iter() {
                let bmkey = (k.clone(), val.clone());
                let mut bmap = self.b_map_r.get_and(&bmkey, |s| s[0].clone()).unwrap();
                if bmap[uid] == 1 as usize {
                    to_return.push(bmkey);
                }
            }

            if to_return.len() > 0 {
                return Some(to_return)
            } else {
                return None
            }
        }
    }

    use std::fmt::Debug;

    // SRMap WriteHandle wrapper structure
    #[derive(Clone)]
    pub struct WriteHandle<K, V, M = ()>
    where
        K: Eq + Hash + Clone + Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {
        handle: SRMap<K, V, M>,
    }

    pub fn new_write<K, V, M>(lock: SRMap<K, V, M>) -> WriteHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {
        WriteHandle { handle: lock }
    }

    impl<K, V, M> WriteHandle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
   {
       // Add the given value to the value-set of the given key.
       pub fn insert(&mut self, k: K, v: V, uid: usize) {
           let mut container = Vec::new();
           container.push(v);
           self.handle.insert(k.clone(), container, uid.clone());
       }

       // Replace the value-set of the given key with the given value.
       pub fn update(&mut self, k: K, v: V, uid: usize) {
           let mut container = Vec::new();
           container.push(v);
           self.handle.insert(k, container, uid.clone());
       }

       // Remove the given value from the value-set of the given key.
       pub fn remove(&mut self, k: K, uid: usize) {
           self.handle.remove(&k, uid.clone());
       }

       pub fn add_user(&mut self, uid: usize) {
           self.handle.add_user(uid.clone());
       }

       pub fn refresh() {
           return
       }

       pub fn empty(&mut self, k: K, uid: usize) {
           self.handle.remove(&k, uid.clone());
       }

       pub fn clear(&mut self, k: K, uid: usize) {
           self.handle.remove(&k, uid.clone());
       }

       pub fn empty_at_index(&mut self, k: K, uid: usize) {
           self.handle.remove(&k, uid.clone());
       }

       pub fn meta_get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<(Option<T>, M)>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           Some((self.handle.get(key, uid).map(move |v| then(&*v)), self.handle.meta.clone()))
       }

       pub fn is_empty(&self) -> bool {
           if self.handle.g_map_size() > 0 {
               return false
           }
           return true
       }


       /// Get the current meta value.
       pub fn meta(&self) -> Option<M> {
          self.with_handle(|inner| inner.meta.clone())
       }

       /// Returns the number of non-empty keys present in the map.
       pub fn len(&self) -> usize {
           self.handle.g_map_size()
       }

       /// Applies a function to the values corresponding to the key, and returns the result.
       pub fn get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<T>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           self.handle.get(key, uid).map(move |v| then(&*v))
       }

       fn with_handle<F, T>(&self, f: F) -> Option<T>
       where
          F: FnOnce(&SRMap<K, V, M>) -> T,
       {
           let res = Some(f(&self.handle));
           res
       }

       /// Read all values in the map, and transform them into a new collection.
       pub fn for_each<F>(&self, mut f: F, uid: usize)
       where
           F: FnMut(&K, &[V]),
       {
           let res = self.handle.get_all(uid).unwrap();
           let mut inner = Vec::new();
           for (k, v) in &res {
               let mut inn = Vec::new();
               inn.push(v.clone());
               inner.push((k.clone(), inn));
           }
           self.with_handle(move |r_handle| {
            for (k, vs) in &inner {
                f(k, &vs[..])
            }
        });
       }

       pub fn contains_key(&self, key: &K, uid: usize) -> bool {
           let res = self.handle.get(key, uid);
           match res {
               Some(r) => true,
               None => false
           }
       }
   }

   unsafe impl<K, V, M> Sync for SRMap<K, V, M>
   where
       K: Eq + Hash + Clone + std::fmt::Debug,
       V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
   {}

   // Constructor for read/write handle tuple
   pub fn construct<K, V, M>(meta_init: M) -> (WriteHandle<K, V, M>, WriteHandle<K, V, M>)
   where
       K: Eq + Hash + Clone + std::fmt::Debug,
       V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
       M: Clone,
    {
        let map = SRMap::<K,V,M>::new(meta_init);
        // let r_handle = new_read(map.clone());
        let w_handle = new_write(map);
        (w_handle.clone(), w_handle)
    }
}


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

    b.iter(|| {
        w.insert(k.clone(), format!("v{}", i), 1);
        i += 1;
    });

    b.iter(|| {
        w.insert(k.clone(), format!("v{}", i), 2);
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

    b.iter(|| {
        r.get_and(&k, |_| false, uid2);
    });
}
