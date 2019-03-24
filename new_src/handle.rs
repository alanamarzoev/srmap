pub mod handle {
    pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
    use std::hash::Hash;
    use std::sync::{Arc, Mutex, RwLock};
    use std::collections::HashMap;

    use evmap;

    #[derive(Clone)]
    pub enum Handle<K, V>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {
        Global { handle: GlobalHandle<K, V> },
        User { handle: UserHandle<K, V> },
    }


    #[derive(Clone)]
    pub struct GlobalHandle<K, V>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {
        pub gmap_r: evmap::ReadHandle<K, (usize, V)>,
        pub gmap_w: Arc<Mutex<evmap::WriteHandle<K, (usize, V)>>>,
        pub largest_uid: Arc<Mutex<usize>>,
        pub largest_rid: Arc<Mutex<usize>>,
    }


    #[derive(Clone)]
    pub struct UserHandle<K, V>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {
        pub gmap_r: evmap::ReadHandle<K, (usize, V)>,
        pub gmap_acl: Arc<RwLock<Vec<usize>>>,
        pub umap: Arc<RwLock<HashMap<K, Vec<V>>>>,
        pub iid: usize,
    }


    impl<K, V> GlobalHandle<K, V>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {

        pub fn gmap_get(&mut self, k: K) -> Option<Vec<(usize, V)>> {
            let mut res_list = Vec::new();
            self.gmap_r.get_and(&k, |set| {
                for v in set {
                    res_list.push(v.clone());
                }
            });

            if res_list.len() > 0 {
                return Some(res_list)
            } else {
                return None
            }
        }


        pub fn gmap_insert(&mut self, k: K, v: V) -> bool {
            let gmap_w = self.gmap_w.lock().unwrap();
            let largest_rid = self.largest_rid.lock().unwrap();
            for val in v.clone() {
                largest_rid += 1;
                gmap_w.insert(k.clone(), (largest_rid.clone(), val.clone()));
            }
            self.gmap_w.refresh();
            return true;
        }


        // gmap: k -> v, v, v', v, v (user only has access to first three v's)
        // gmap_acl: (k,v) -> count of number of these records you have access to, (k,v) not present in the map implies count = 0
        // in this example: (k, v) -> 3

        pub fn insert(&mut self, k: K, v: V) -> bool {
            self.gmap_insert(k, v);
        }

        /// Applies a function to the values corresponding to the key, and returns the result.
        pub fn get_and<F, T>(&self, k: &K, then: F) -> Option<T>
        where
            K: Hash + Eq,
            F: FnOnce(&[V]) -> T,
        {

            let mut result_list = Vec::new();

            self.g_map_r.get_and(&k, |set| {
                for v in set {
                    match self.umap.get(&(k, v)) {
                        result_list.push(v.1);
                    }
                }
            });

            if result_list.len() < 1 {
                return None;
            } else {
                let mut result_list = Some(result_list).map(move |v| then(&*v)).unwrap();
                Some(result_list)
            }

        }

        /// Applies a function to the values corresponding to the key, and returns the result.
        pub fn meta_get_and<F, T>(&self, k: &K, then: F) -> Option<(Option<T>, M)>
        where
            K: Hash + Eq,
            F: FnOnce(&[V]) -> T,
        {
            let mut result_list = Vec::new();

            self.g_map_r.get_and(&k, |set| {
                for v in set {
                    match self.umap.get(&(k, v)) {
                        result_list.push(v.1);
                    }
                }
            });

            if result_list.len() < 1 {
                return None;
            } else {
                let mut result_list = Some(result_list).map(move |v| then(&*v)).unwrap();
                Some(result_list, None)
            }

        }


       // Replace the value-set of the given key with the given value.
       pub fn update(&mut self, k: K, v: V) {
          self.remove(k.clone());
          self.insert(k, v);

       }

       // Remove the given value from the value-set of the given key.
       pub fn remove(&mut self, k: K) {
           let mut gmap_w = self.gmap_w.write().unwrap();
           gmap_w.remove(k);
       }

       pub fn refresh() {
           return
       }

       pub fn empty(&mut self, k: K) {
           self.remove(&k);
       }

       pub fn clear(&mut self, k: K) {
           self.remove(&k);
       }

       pub fn empty_at_index(&mut self, k: K) {
           self.remove(&k);
       }

       /// Get the current meta value.
       pub fn meta(&self) -> Option<M> {
          return None;
       }

       // fn with_handle<F, T>(&self, f: F) -> Option<T>
       // where
       //    F: FnOnce(&SRMap<K, V, M>) -> T,
       // {
       //     let res = Some(f(&self.handle));
       //     res
       // }

       /// Read all values in the map, and transform them into a new collection.
       pub fn for_each<F>(&self, mut f: F)
       where
           F: FnMut(&K, &[V]),
       {
        //    let res = self.handle.get_all(self.iid).unwrap();
        //    let mut inner = Vec::new();
        //    for (k, v) in &res {
        //        let mut inn = Vec::new();
        //        inn.push(v.clone());
        //        inner.push((k.clone(), inn));
        //    }
        //    self.with_handle(move |_| {
        //     for (k, vs) in &inner {
        //         f(k, &vs[..])
        //     }
        // });
       }

       pub fn contains_key(&self, k: &K) -> bool {
           self.get_and(&k, |set| {
               if set.len() > 0 {
                   return true;
               } else {
                   return false;
               }
           })
        }
    }




    impl<K, V> UserHandle<K, V>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    {
        pub fn gmap_get(&mut self, k: K) -> Option<Vec<V>> {
            let mut res_list = Vec::new();
            self.gmap_r.get_and(&k, |set| {
                for v in set {
                    res_list.push(v.clone());
                }
            });

            if res_list.len() > 0 {
                return Some(res_list)
            } else {
                return None
            }
        }


        pub fn umap_insert(&mut self, k: K, v: V) {
            let mut umap = self.umap.write().unwrap();

            let mut add = false;
            let mut added_vec = None;

            match umap.get_mut(&k) {
                Some(vec) => {
                    vec.push(v.clone());
                },
                None => {
                    let mut new_vec = Vec::new();
                    new_vec.push(v.clone());
                    add = true;
                    added_vec = Some(new_vec);
                }
            }

            if add {
                umap.insert(k.clone(), added_vec.unwrap());
            }
        }


        // gmap: k -> v, v, v', v, v (user only has access to first three v's)
        // gmap_acl: (k,v) -> count of number of these records you have access to, (k,v) not present in the map implies count = 0
        // in this example: (k, v) -> 3
        pub fn insert(&mut self, k: K, v: V) {
            let found = self.gmap_get(k);
            match found {
                Some(results) => {
                    let mut found_in_gmap = false;
                    for val in &v {
                        for (rid, res) in &results {
                            if val == res {
                                // update count of index rid in gmap_acl
                                let mut acl = self.gmap_acl.write().unwrap();
                                let diff = rid - acl.len();
                                if diff > 0 {
                                    for i in 0..diff {
                                        acl.push(0);
                                    }
                                }
                                acl[rid] = 1;
                                found_in_gmap = true;
                            }
                        }
                    }

                    // couldn't find in gmap, so update umap
                    if !found_in_gmap {
                        self.umap_insert(k, v);
                    }
                },
                None => {
                    // insert directly into umap
                    self.umap_insert(k, v);
                }
            }
        }


        /// Applies a function to the values corresponding to the key, and returns the result.
        pub fn get_and<F, T>(&self, k: &K, then: F) -> Option<T>
        where
            K: Hash + Eq,
            F: FnOnce(&[V]) -> T,
        {

            let mut result_list = Vec::new();

            // get records stored in umap
            let mut umap_res = self.umap.write().unwrap();
            let mut umap_res = umap_res.get_mut(k);

            match umap_res {
                Some(res) => {
                    result_list = res;
                },
                None => {}
            }

            let mut gmap_acl = self.gmap_acl.write().unwrap();

            self.g_map_r.get_and(&k, |set| {
                for (rid, v) in set {
                    if gmap_acl[rid] == 1 {
                        result_list.push(v.clone());
                    }
                }
            });

            if result_list.len() < 1 {
                return None;
            } else {
                let mut result_list = Some(result_list).map(move |v| then(&*v)).unwrap();
                Some(result_list)
            }

        }

        /// Applies a function to the values corresponding to the key, and returns the result.
        pub fn meta_get_and<F, T>(&self, k: &K, then: F) -> Option<(Option<T>, M)>
        where
            K: Hash + Eq,
            F: FnOnce(&[V]) -> T,
        {
            let mut result_list = Vec::new();

            // get records stored in umap
            let mut umap_res = self.umap.write().unwrap();
            let mut umap_res = umap_res.get_mut(k);

            match umap_res {
                Some(res) => {
                    result_list = res;
                },
                None => {}
            }

            let mut gmap_acl = self.gmap_acl.write().unwrap();

            self.g_map_r.get_and(&k, |set| {
                for (rid, v) in set {
                    if gmap_acl[rid] == 1 {
                        result_list.push(v.clone());
                    }
                }
            });

            if result_list.len() < 1 {
                return None;
            } else {
                let mut result_list = Some(result_list).map(move |v| then(&*v)).unwrap();
                Some(result_list, None)
            }
        }


       // Replace the value-set of the given key with the given value.
       pub fn update(&mut self, k: K, v: V) {
          // self.remove(k.clone());
          // self.insert(k, v);

       }

       // Remove the given value from the value-set of the given key.
       pub fn remove(&mut self, k: K) {
           // let mut gmap_acl = self.gmap_acl.write().unwrap();
           // let mut to_remove = Vec::new();
           //
           // for (k, v) in &gmap_acl {
           //     if k.0 == k {
           //         to_remove.push(k);
           //     }
           // }
           //
           // for item in &to_remove {
           //     gmap_acl.remove(item);
           // }
           //
           // let mut umap = self.umap.write().unwrap();
           // umap.remove(k);
       }

       pub fn refresh() {
           return
       }

       pub fn empty(&mut self, k: K) {
           self.remove(&k);
       }

       pub fn clear(&mut self, k: K) {
           self.remove(&k);
       }

       pub fn empty_at_index(&mut self, k: K) {
           self.remove(&k);
       }

       /// Get the current meta value.
       pub fn meta(&self) -> Option<M> {
          return None;
       }

       // fn with_handle<F, T>(&self, f: F) -> Option<T>
       // where
       //    F: FnOnce(&SRMap<K, V, M>) -> T,
       // {
       //     let res = Some(f(&self.handle));
       //     res
       // }

       /// Read all values in the map, and transform them into a new collection.
       pub fn for_each<F>(&self, mut f: F)
       where
           F: FnMut(&K, &[V]),
       {
        //    let res = self.handle.get_all(self.iid).unwrap();
        //    let mut inner = Vec::new();
        //    for (k, v) in &res {
        //        let mut inn = Vec::new();
        //        inn.push(v.clone());
        //        inner.push((k.clone(), inn));
        //    }
        //    self.with_handle(move |_| {
        //     for (k, vs) in &inner {
        //         f(k, &vs[..])
        //     }
        // });
       }

       pub fn contains_key(&self, k : &K) -> bool {
           self.get_and(&k, |set| {
               if set.len() > 0 {
                   return true;
               } else {
                   return false;
               }
           })
        }
    }
}
