pub mod handle {
    pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
    use std::hash::Hash;
    use std::sync::{Arc, RwLock};
    use std::collections::HashMap;

    use evmap;
    use inner::srmap::SRMap;

    #[derive(Clone)]
    pub struct Handle<K, V, M = ()>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {
        pub handle: SRMap<K, V, M>,
        pub iid: usize,
        pub umap: Arc<RwLock<HashMap<K, Vec<V>>>>
    }

    impl<K, V, M> Handle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {

       pub fn clone_new_user(&mut self) -> (Handle<K, V, M>, Handle<K, V, M>) {
           let mut umap = Arc::new(RwLock::new(HashMap::new()));
           let mut new_handle = Handle {
               handle: self.handle.clone(),
               iid: 0,
               umap: umap,
           };

           new_handle.add_user();

           println!("new handle with uid: {}", new_handle.iid);
           (new_handle.clone(), new_handle)
       }


       // Add the given value to the value-set of the given key.
       pub fn insert(&mut self, k: K, v: V) {
           let mut container = Vec::new();
           container.push(v.clone());
           let success = self.handle.insert(k.clone(), container, self.iid);
           println!("no matching value in gmap. inserting into user {}'s umap...", self.iid);

           // insert into umap if gmap insert didn't succeed
           if !success {
               let mut add = false;
               let mut added_vec = None;

               match self.umap.write().unwrap().get_mut(&k){
                   Some(vec) => { vec.push(v.clone()); },
                   None => {
                       let mut new_vec = Vec::new();
                       new_vec.push(v.clone());
                       add = true;
                       added_vec = Some(new_vec);
                   }
               }

               if add {
                   self.umap.write().unwrap().insert(k.clone(), added_vec.unwrap());
               }
           }
       }


       // Replace the value-set of the given key with the given value.
       pub fn update(&mut self, k: K, v: V) {
           let mut container = Vec::new();
           container.push(v);
           self.handle.insert(k, container, self.iid);
       }

       // Remove the given value from the value-set of the given key.
       pub fn remove(&mut self, k: K) {
           self.handle.remove(&k, self.iid);
       }

       pub fn add_user(&mut self) {
           self.iid = self.handle.add_user();
       }

       pub fn refresh() {
           return
       }

       pub fn empty(&mut self, k: K) {
           self.handle.remove(&k, self.iid);
       }

       pub fn clear(&mut self, k: K) {
           self.handle.remove(&k, self.iid);
       }

       pub fn empty_at_index(&mut self, k: K) {
           self.handle.remove(&k, self.iid);
       }

       pub fn meta_get_and<F, T>(&self, key: &K, then: F) -> Option<(Option<T>, M)>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           // TODO parallelize this!!! gmap and umap reads should happen at the same time

           // get records stored in umap
           let mut umap_res = self.umap.write().unwrap().get_mut(key).unwrap().clone();
           let mut gmap_res = self.handle.get(key, self.iid).unwrap();

           umap_res.append(&mut gmap_res);

           let mut umap_res = Some(umap_res).map(move |v| then(&*v)).unwrap();

           // clone meta
           let meta = self.handle.meta.clone();

           Some((Some(umap_res), meta))
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
       pub fn get_and<F, T>(&self, key: &K, then: F) -> Option<T>
       where
           K: Hash + Eq,
           F: FnOnce(&[V]) -> T,
       {
           // get records stored in umap
           let mut umap_res = self.umap.write().unwrap().get_mut(key).unwrap().clone();
           let mut gmap_res = self.handle.get(key, self.iid).unwrap();

           umap_res.append(&mut gmap_res);

           let mut umap_res = Some(umap_res).map(move |v| then(&*v)).unwrap();

           Some(umap_res)
       }


       fn with_handle<F, T>(&self, f: F) -> Option<T>
       where
          F: FnOnce(&SRMap<K, V, M>) -> T,
       {
           let res = Some(f(&self.handle));
           res
       }

       /// Read all values in the map, and transform them into a new collection.
       pub fn for_each<F>(&self, mut f: F)
       where
           F: FnMut(&K, &[V]),
       {
           let res = self.handle.get_all(self.iid).unwrap();
           let mut inner = Vec::new();
           for (k, v) in &res {
               let mut inn = Vec::new();
               inn.push(v.clone());
               inner.push((k.clone(), inn));
           }
           self.with_handle(move |_| {
            for (k, vs) in &inner {
                f(k, &vs[..])
            }
        });
       }

       pub fn contains_key(&self, key: &K) -> bool {
           let res = self.handle.get(key, self.iid);
           match res {
               Some(_r) => true,
               None => false
           }
       }

    }
}
