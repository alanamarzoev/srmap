pub mod handle {
    pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
    use std::hash::Hash;
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
    }

    impl<K, V, M> Handle<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {

       // Add the given value to the value-set of the given key.
       pub fn insert(&mut self, k: K, v: V) {
           let mut container = Vec::new();
           container.push(v);
           self.handle.insert(k.clone(), container, self.iid);
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

       pub fn add_user(&mut self) {
           self.iid = self.handle.add_user();
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
           self.with_handle(move |_| {
            for (k, vs) in &inner {
                f(k, &vs[..])
            }
        });
       }

       pub fn contains_key(&self, key: &K, uid: usize) -> bool {
           let res = self.handle.get(key, uid);
           match res {
               Some(_r) => true,
               None => false
           }
       }

    }
}
