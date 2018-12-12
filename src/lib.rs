#![feature(trivial_bounds)]
#![feature(extern_prelude)]
#![feature(test)]
#![feature(try_from)]

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate evmap;
extern crate test;
extern crate time;
pub mod data;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate arccstr;
extern crate nom_sql;
extern crate rand;

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
    pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
    use std::cell::RefCell;

    // SRMap inner structure
    pub struct SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {
        pub g_map_r: evmap::ReadHandle<K, V>,
        pub b_map_r: evmap::ReadHandle<(K, V), Vec<usize>>,
        pub global_w: Arc<
            Mutex<(
                evmap::WriteHandle<K, V>,
                evmap::WriteHandle<(K, V), Vec<usize>>,
            )>,
        >,
        pub u_map: Arc<RwLock<HashMap<K, Vec<V>>>>,
        pub id_store: Arc<RwLock<HashMap<usize, usize>>>,
        pub meta: M,
        largest: usize,
        g_records: usize,
        log: slog::Logger,
        initialized: bool,
    }

    impl<K, V, M> Clone for SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
        M: Clone,
    {
        fn clone(&self) -> Self {
            let logger = super::logger_pls();
            SRMap {
                g_map_r: self.g_map_r.clone(),
                b_map_r: self.b_map_r.clone(),
                global_w: self.global_w.clone(),
                id_store: self.id_store.clone(),
                largest: self.largest.clone(),
                u_map: self.u_map.clone(),
                meta: self.meta.clone(),
                g_records: self.g_records.clone(),
                initialized: self.initialized.clone(),
                log: logger,
            }
        }
    }

    pub fn update_access(bitmap: Vec<usize>, uid: usize, add: bool) -> Vec<usize> {
        let index = uid / 64;
        let offset = uid % 64;
        let bmap_len = bitmap.len();
        let mut updated_map = bitmap;
        if bmap_len <= index {
            // extend the bitmap lazily to accommodate all users.
            if add {
                let num_new_elements = index - (bmap_len - 1);
                for el in 0..num_new_elements {
                    updated_map.push(0);
                }
            }
            // if this was an access retraction and that portion of the bitmap never existed,
            // then there wasn't ever access to begin with. this will change when we do
            // compression.
        }

        let access = 1 << offset;
        updated_map[index] = updated_map[index] ^ access; // or do i use an or and??? check this

        return updated_map
    }

    pub fn get_access(bitmap: Vec<usize>, uid: usize) -> bool {
        let index = uid / 64;
        let offset = uid % 64;
        let bmap_len = bitmap.len();
        if bmap_len <= index {
            return false
        }

        let mask = 1 << offset;
        let res = bitmap[index] & mask;
        if res == 0 {
            return false
        } else {
            return true
        }
    }

    impl<K, V, M> SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
        M: Clone,
    {
        pub fn new(init_m: M) -> SRMap<K, V, M> {
            let logger = super::logger_pls();
            let (g_map_r, mut g_map_w) = evmap::new();
            let (b_map_r, mut b_map_w) = evmap::new();
            SRMap {
                g_map_r: g_map_r,
                global_w: Arc::new(Mutex::new((g_map_w, b_map_w))),
                b_map_r: b_map_r,
                u_map: Arc::new(RwLock::new(HashMap::new())),
                id_store: Arc::new(RwLock::new(HashMap::new())),
                meta: init_m,
                g_records: 0,
                largest: 0 as usize,
                log: logger,
                initialized: false,
            }
        }

        pub fn g_map_size(&self) -> usize {
            let mut gm_vec = Vec::new();
            self.g_map_r.for_each(|x, s| gm_vec.push(1));
            gm_vec.len()
        }

        pub fn get_id(&self, uid: usize) -> Option<usize> {
            match self.id_store.read().unwrap().get(&uid) {
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

            // global map insert.
            if (uid == (0 as usize)) {
                // println!("global insert");
                for val in v {

                    self.g_records += 1;

                    // Add (index, value) to global map
                    g_map_w.insert(k.clone(), val.clone());

                    // create new bitmap! no users start off having access except for the global
                    // universe.
                    let mut bit_map = Vec::new();
                    bit_map.push(0);

                    b_map_w.insert((k.clone(), val.clone()), bit_map);
                }
                g_map_w.refresh();
                b_map_w.refresh();

            } else {
                // if value exists in the global map, remove this user's name from restricted access list.
                // otherwise, add record to the user's umap.
                let mut u_map = self.u_map.write().unwrap();
                let mut added = false;
                let mut same_as_global = self.g_map_r.get_and(&k.clone(), |vs| {
                    for val in &v {
                        let mut last_seen = 0;
                        let mut count = 0 as usize;
                        let mut found = false;
                        let mut bmap : Vec<Vec<usize>> = Vec::new();

                        // attempting to find a match for this value in the global map
                        // _that this user does not yet have access to_. if this is successful,
                        // indicate that the value has been found, and update access.
                        // if not successful, insert into umap. repeat for all values.
                        for v in vs {
                            if *v == *val && count >= last_seen && found == false {
                                self.b_map_r.get_and(&(k.clone(), val.clone()), |s| {
                                    // if user doesn't yet have access to a record with a matching
                                    // value in the global map, then update this bitmap to grant
                                    // access. otherwise, add to user map.
                                    let r = s[count].clone();
                                    match get_access(s[count].clone().to_vec(), uid) {
                                        true => {
                                            last_seen = count;
                                        },
                                        false => {
                                            found = true;
                                            bmap = s.clone().to_vec();
                                        }
                                    }

                                    if !found {
                                        count = count + 1 as usize;
                                    }
                                }
                            );}
                        }

                        if found {
                            // give access
                            // println!("flipping bit");
                            bmap[count] = update_access(bmap[count].clone().to_vec(), uid, true);

                            let bmkey = (k.clone(), val.clone());
                            b_map_w.clear(bmkey.clone());

                            // update the shared bmap
                            for v in &bmap {
                                b_map_w.insert(bmkey.clone(), v.clone());
                            }

                            b_map_w.refresh();

                        } else {
                            // println!("umap insert");
                            // insert into umap
                            let mut add = false;
                            let mut added_vec = None;

                            match u_map.get_mut(&k){
                                Some(vec) => { vec.push(val.clone()); },
                                None => {
                                    let mut new_vec = Vec::new();
                                    new_vec.push(val.clone());
                                    add = true;
                                    added_vec = Some(new_vec);
                                }
                            }

                            if add {
                                u_map.insert(k.clone(), added_vec.unwrap());
                            }
                        }
                    }
                });
            }
        }


        pub fn get(&self, k: &K, id: usize) -> Option<Vec<V>> {
            let mut id = self.get_id(id.clone());
            let mut uid = 0;
            if id == None {
                return None
            } else {
                uid = id.unwrap();
            }

            let mut u_map = self.u_map.write().unwrap();
            let mut v_vec = u_map.get_mut(k);

            let mut res_list : Vec<V>;
            if v_vec != None {
                res_list = v_vec.unwrap().clone();
            } else {
                res_list = Vec::new();
            }

            self.g_map_r.get_and(&k, |set| {
                for v in set {
                    let bmap = self
                        .b_map_r
                        .get_and(&(k.clone(), v.clone()), |s| s[0].clone())
                        .unwrap();
                    if get_access(bmap, uid) {
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

            let mut u_map = self.u_map.write().unwrap();
            u_map.remove(k);

            self.g_map_r.get_and(&k, |set| {
                for v in set.iter() {
                    let bm_key = &(k.clone(), v.clone());
                    let mut bmap = self.b_map_r.get_and(bm_key, |s| s[0].clone());
                    match bmap {
                        Some(mut bm) => {
                            bm = update_access(bm, uid, false);
                        }
                        None => {}
                    }
                }
            });
        }


        pub fn add_user(&mut self, uid: usize) -> Option<usize> {
            // add to ID store
            if self.initialized {
                self.largest += 1;
            }

            self.initialized = true;
            let (ref mut g_map_w, ref mut b_map_w) = *self.global_w.lock().unwrap();

            self.id_store.write().unwrap().insert(uid.clone(), self.largest.clone());
            Some(self.largest.clone()) // return internal id
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

            let mut u_map = self.u_map.write().unwrap();
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

                if get_access(bmap, uid) {
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
    pub struct Handle<K, V, M = ()>
    where
        K: Eq + Hash + Clone + Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {
        handle: SRMap<K, V, M>,
        iid: Option<usize>,
    }

    pub fn new<K, V, M>(lock: SRMap<K, V, M>) -> Handle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {
        Handle { handle: lock, iid: None }
    }

    impl<K, V, M> Handle<K, V, M>
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
           self.iid = self.handle.add_user(uid.clone());
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
           // println!("start of meta get and in srmap");
           let meta = self.handle.meta.clone();
           Some((self.handle.get(key, uid).map(move |v| then(&*v)), meta.clone()))
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
       M: Clone,
   {}

   // Constructor for read/write handle tuple
   pub fn construct<K, V, M>(meta_init: M) -> (Handle<K, V, M>, Handle<K, V, M>)
   where
       K: Eq + Hash + Clone + std::fmt::Debug,
       V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
       M: Clone,
    {
        let map = SRMap::<K,V,M>::new(meta_init);
        let w_handle = new(map.clone());
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

use data::DataType;
fn get_posts(num: usize) -> Vec<Vec<DataType>> {
    let mut rng = rand::thread_rng();
    let mut records : Vec<Vec<DataType>> = Vec::new();
    for i in 0..num {
        let pid = i.into();
        let author = (0 as usize).into();
        let cid = (0 as usize).into();
        let content : DataType = format!("post #{}", i).into();
        let private = (0 as usize).into();
        let anon = 1.into();
        records.push(vec![pid, cid, author, content, private, anon]);
    }
    records
}


fn get_private_posts(num: usize, uid: usize) -> Vec<Vec<DataType>> {
    let mut rng = rand::thread_rng();
    let mut records : Vec<Vec<DataType>> = Vec::new();
    for i in 0..num {
        let pid = i.into();
        let author = (uid.clone() as usize).into();
        let cid = (0 as usize).into();
        let content : DataType = format!("post #{}", (i + uid)).into();
        let private = (0 as usize).into();
        let anon = 1.into();
        records.push(vec![pid, cid, author, content, private, anon]);
    }
    records
}

#[bench]
fn bench_insert_multival(b: &mut Bencher) {
    use time::{Duration, PreciseTime};
    use rand;
    use rand::Rng;
    pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};

    let (_r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);

    let num_users = 10;
    let num_posts = 1000;
    let num_private = 0;

    // create users
    let mut j = 0;
    while j < num_users {
        w.add_user(j as usize);
        j += 1;
    }

    // add records to global map
    let k : DataType = "x".to_string().into();
    let mut avg = Duration::nanoseconds(0);

    let mut recs = get_posts(num_posts as usize);
    for i in recs {
        let start = PreciseTime::now();
        w.insert(k.clone(), i, 0 as usize);
        let delta = start.to(PreciseTime::now());
        avg = (avg + delta);
    }

    // update bitmaps for users sharing global values
    let mut avg = Duration::nanoseconds(0);
    for j in 1..num_users + 1 {
        // insert public posts for each user
        let mut recs = get_posts(num_posts as usize);
        for i in recs {
            w.insert(k.clone(), i, j as usize);
        }
        // insert private posts for each user
        let mut recs = get_private_posts(num_private as usize, j as usize);
        for i in recs {
            w.insert(k.clone(), i, j as usize);
        }
    }

    for j in 0..num_users + 1 {
        // insert public posts for each user
        let mut res = w.get_and(&k.clone(), |s| s.len().clone(), j as usize);
        // println!("USER: {:?}, NUM_RECS: {:?}", j, res.clone());
    }


    // let num_avg = avg.num_nanoseconds().unwrap() / i;
    // println!("avg time: {:?}", num_avg.clone());

    // not in global map, umap updates
    // let mut i = 0;
    // let mut avg = Duration::nanoseconds(0);
    // while i < 1000 {
    //     let start = PreciseTime::now();
    //     w.insert(k.clone(), format!("{}", i), uid3);
    //     let delta = start.to(PreciseTime::now());
    //     i += 1;
    //     avg = (avg + delta);
    // }
    // let num_avg = avg.num_nanoseconds().unwrap() / i;
    // println!("avg time: {:?}", num_avg.clone());
}

// #[bench]
// fn basic_clone_test(b: &mut Bencher) {
//
//     let (r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);
//
//     // add records to global map
//     let k : DataType = "x".to_string().into();
//
//     let mut recs = get_posts(2 as usize);
//     for i in recs {
//         w.insert(k.clone(), i, 0 as usize);
//     }
//     let mut res_vec = Vec::new();
//     r.get_and(&k, |s| res_vec.push(s.len()), 0 as usize);
//     println!("{:?}", res_vec);
//
// }

//
// #[bench]
// fn bench_get_throughput(b: &mut Bencher) {
//     let (_r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);
//
//     let num_users = 10;
//     let num_posts = 9000;
//     let num_private = 1000;
//
//     // create users
//     let mut j = 0;
//     while j < num_users {
//         w.add_user(j as usize);
//         j += 1;
//     }
//
//     let k = "x".to_string();
//     let v = "x".to_string();
//
//     w.insert(k.clone(), v.clone(), uid1);
//
//     b.iter(|| {
//         r.get_and(&k, |_| false, uid1);
//     });
//
//     b.iter(|| {
//         r.get_and(&k, |_| false, uid2);
//     });
// }
