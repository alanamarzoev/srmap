#![feature(trivial_bounds)]
#![feature(test)]
#![feature(try_from)]
#![feature(box_patterns)]

extern crate evmap;
extern crate serde;
extern crate slog;
extern crate slog_term;
extern crate test;
extern crate time;
extern crate bit_vec; 

#[macro_use]
extern crate serde_derive;
extern crate arccstr;
extern crate chrono;
extern crate nom_sql;
extern crate rand;

pub mod data;
pub mod handle;
pub mod inner;

use handle::handle::Handle;
use inner::srmap::SRMap;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};

pub fn new<K, V, M>(lock: SRMap<K, V, M>) -> Handle<K, V, M>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    M: Clone,
{
    let umap = Arc::new(RwLock::new(HashMap::new()));
    Handle {
        handle: lock,
        iid: 0,
        umap: umap,
    }
}

// Constructor for read/write handle tuple
pub fn construct<K, V, M>(meta_init: M) -> (Handle<K, V, M>, Handle<K, V, M>)
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    M: Clone,
{
    let map = SRMap::<K, V, M>::new(meta_init);
    let mut w_handle = new(map.clone());
    // adds user with uid 0...
    w_handle.add_user();
    (w_handle.clone(), w_handle)
}
