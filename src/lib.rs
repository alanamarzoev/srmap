#![feature(trivial_bounds)]
#![feature(extern_prelude)]
#![feature(test)]

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate evmap;
extern crate test;
extern crate time;

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
        initialized: bool,
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
                initialized: false
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
                    self.g_records += 1;
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

                    // println!("global: inserting k: {:?} v: {:?} bmap: {:?}", k.clone(), val.clone(), bit_map.clone());

                    b_map_w.insert((k.clone(), val.clone()), bit_map);
                }
                g_map_w.refresh();
                b_map_w.refresh();
                // println!("num grecords: {:?}", self.g_records.clone());

            } else {
                // println!("here... ");
                // If value exists in global map and isn't accessible -> flip a bit.
                // Otherwise, add the value to the user's map.
                let mut u_map = self.u_map[uid].write().unwrap();
                let mut added = false;
                let mut same_as_global = self.g_map_r.get_and(&k.clone(), |vs| {
                    for val in &v {
                        let mut last_seen = 0;
                        let mut count = 0 as usize;
                        let mut found = false;
                        let mut bmap : Vec<Vec<usize>> = Vec::new();
                        // let mut add_user = false;
                        // println!("VS len : {:?}", vs.len().clone());
                        for v in vs {
                            if *v == *val && count >= last_seen && found == false {
                                self.b_map_r.get_and(&(k.clone(), val.clone()), |s| {
                                    //println!("s: {:?}", s.clone());
                                //                                                       println!("count: {:?} uid: {:?}", count.clone(), uid.clone());
                                //                                                       println!("k: {:?} v: {:?}", k.clone(), v.clone());
                                                                                      if s[count][uid] == 0 { found = true;
                                                                                                              bmap = s.to_vec().clone();
                                                                                                             }
                                                                                                             else { last_seen = count; }});
                                if found {
                                    // println!("breaking at count {:?}", count.clone());
                                    break;
                                    // println!("shouldn't happen");
                                } else {
                                    count = count + 1 as usize;
                                }
                            }
                        }

                        if found {
                            bmap[count][uid] = 1 as usize;
                            let bmkey = (k.clone(), val.clone());
                            b_map_w.clear(bmkey.clone());
                            for v in &bmap {
                                b_map_w.insert(bmkey.clone(), v.clone());
                            }
                            // println!("updated bitmap: {:?}", bmap.clone());
                            b_map_w.refresh();
                        } else {
                            match u_map.get_mut(&k){
                                Some(vec) => vec.push(val.clone()),
                                None => { let mut new_vec = Vec::new(); new_vec.push(val.clone()); }
                            }
                        }
                    }
                });
            }
        }


        pub fn get(&self, k: &K, id: usize) -> Option<Vec<V>> { //TODO optimize this!! will prob be slow
            let mut id = self.get_id(id.clone());
            let mut uid = 0;
            // println!("in get");
            if id == None {
                return None
            } else {
                uid = id.unwrap();
            }

            let mut u_map = self.u_map[uid].write().unwrap();
            // println!("acquired umap");

            let mut v_vec = u_map.get_mut(k);
            let mut res_list : Vec<V>;
            if v_vec != None {
                res_list = v_vec.unwrap().clone();
            } else {
                res_list = Vec::new();
            }

            // println!("call to evmap get_and");
            self.g_map_r.get_and(&k, |set| {
                for v in set {
                    // println!("call to bmap srmap get_and");
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
            // println!("about to return vec of records");
            if to_return.len() > 0 {
                // println!("returning some");
                return Some(to_return)
            } else {
                // println!("returning none");
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
            if self.initialized {
                self.largest += 1;
            }

            self.initialized = true;
            // create user map
            let mut um = Arc::new(RwLock::new(HashMap::new()));
            self.u_map.push(um);

            let (ref mut g_map_w, ref mut b_map_w) = *self.global_w.lock().unwrap();

            // println!("Adding umap for user with uid: {:?} internal: {:?}, len of umap vec: {:?}", uid.clone(), self.largest.clone(), self.u_map.len());
            b_map_w.refresh();

            // add bitmap flag for this user in every global bitmap
            let mut new_bm = Vec::new();
            self.b_map_r.for_each(|k, v| { new_bm.push((k.clone(), v[0].clone())) }); // TODO Change to get all

            for y in new_bm.iter() {
                b_map_w.clear(y.0.clone());
            }

            // println!("adding user {:?} to {:?} record bms", uid.clone(), new_bm.len().clone());
            for y in new_bm.iter() {
                let mut kv = y.0.clone();
                let mut v = y.1.clone();
                v.push(0);
                // println!("k: {:?} new bm: {:?}", kv.clone(), v.clone());
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
        handle: Arc<Mutex<SRMap<K, V, M>>>,
    }

    pub fn new_write<K, V, M>(lock: Arc<Mutex<SRMap<K, V, M>>>) -> WriteHandle<K, V, M>
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
           self.handle.lock().unwrap().insert(k.clone(), container, uid.clone());
       }

       // Replace the value-set of the given key with the given value.
       pub fn update(&mut self, k: K, v: V, uid: usize) {
           let mut container = Vec::new();
           container.push(v);
           self.handle.lock().unwrap().insert(k, container, uid.clone());
       }

       // Remove the given value from the value-set of the given key.
       pub fn remove(&mut self, k: K, uid: usize) {
           self.handle.lock().unwrap().remove(&k, uid.clone());
       }

       pub fn add_user(&mut self, uid: usize) {
           self.handle.lock().unwrap().add_user(uid.clone());
       }

       pub fn refresh() {
           return
       }

       pub fn empty(&mut self, k: K, uid: usize) {
           self.handle.lock().unwrap().remove(&k, uid.clone());
       }

       pub fn clear(&mut self, k: K, uid: usize) {
           self.handle.lock().unwrap().remove(&k, uid.clone());
       }

       pub fn empty_at_index(&mut self, k: K, uid: usize) {
           self.handle.lock().unwrap().remove(&k, uid.clone());
       }

       pub fn meta_get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<(Option<T>, M)>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           // println!("start of meta get and in srmap");
           let meta = self.handle.lock().unwrap().meta.clone();
           Some((self.handle.lock().unwrap().get(key, uid).map(move |v| then(&*v)), meta.clone()))
       }

       pub fn is_empty(&self) -> bool {
           if self.handle.lock().unwrap().g_map_size() > 0 {
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
           self.handle.lock().unwrap().g_map_size()
       }

       /// Applies a function to the values corresponding to the key, and returns the result.
       pub fn get_and<F, T>(&self, key: &K, then: F, uid: usize) -> Option<T>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           self.handle.lock().unwrap().get(key, uid).map(move |v| then(&*v))
       }

       fn with_handle<F, T>(&self, f: F) -> Option<T>
       where
          F: FnOnce(&SRMap<K, V, M>) -> T,
       {
           let res = Some(f(&self.handle.lock().unwrap()));
           res
       }

       /// Read all values in the map, and transform them into a new collection.
       pub fn for_each<F>(&self, mut f: F, uid: usize)
       where
           F: FnMut(&K, &[V]),
       {
           let res = self.handle.lock().unwrap().get_all(uid).unwrap();
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
           let res = self.handle.lock().unwrap().get(key, uid);
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
        let map = Arc::new(Mutex::new(SRMap::<K,V,M>::new(meta_init)));
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
    use time::{Duration, PreciseTime};

    let uid1: usize = 0 as usize;
    let uid2: usize = 1 as usize;
    let uid3: usize = 2 as usize;

    let (_r, mut w) = srmap::construct::<String, String, Option<i32>>(None);

    // create two users
    w.add_user(uid1);
    w.add_user(uid2);

    let k = "x".to_string();

    let mut i = 0;
    let mut avg = Duration::nanoseconds(0);
    // global map updates
    while i < 1000 {
        let start = PreciseTime::now();
        w.insert(k.clone(), format!("v{}", i), uid1);
        let delta = start.to(PreciseTime::now());
        i += 1;
        avg = (avg + delta);
    }
    let num_avg = avg.num_nanoseconds().unwrap() / i;
    println!("avg time: {:?}", num_avg.clone());

    // values in global map, bitmap updates
    let mut i = 0;
    let mut avg = Duration::nanoseconds(0);
    while i < 1000 {
        let start = PreciseTime::now();
        w.insert(k.clone(), format!("v{}", i), uid2);
        let delta = start.to(PreciseTime::now());
        i += 1;
        avg = (avg + delta);
    }
    let num_avg = avg.num_nanoseconds().unwrap() / i;
    println!("avg time: {:?}", num_avg.clone());

    // not in global map, umap updates
    let mut i = 0;
    let mut avg = Duration::nanoseconds(0);
    while i < 1000 {
        let start = PreciseTime::now();
        w.insert(k.clone(), format!("{}", i), uid3);
        let delta = start.to(PreciseTime::now());
        i += 1;
        avg = (avg + delta);
    }
    let num_avg = avg.num_nanoseconds().unwrap() / i;
    println!("avg time: {:?}", num_avg.clone());
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
