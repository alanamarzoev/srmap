#![feature(trivial_bounds)]
#![feature(test)]
#![feature(try_from)]

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate evmap;
extern crate test;
extern crate time;
extern crate serde;

#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate arccstr;
extern crate nom_sql;
extern crate rand;

pub mod inner;
pub mod data;
pub mod handle;

use handle::handle::Handle;
use inner::srmap::SRMap;

use std::hash::Hash;
pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};


pub fn new<K, V, M>(lock: SRMap<K, V, M>) -> Handle<K, V, M>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
    M: Clone,
{
    Handle { handle: lock, iid: 0 }
}

// Constructor for read/write handle tuple
pub fn construct<K, V, M>(meta_init: M) -> (Handle<K, V, M>, Handle<K, V, M>)
where
   K: Eq + Hash + Clone + std::fmt::Debug,
   V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
   M: Clone,
{
    let map = SRMap::<K,V,M>::new(meta_init);
    let mut w_handle = new(map.clone());
    w_handle.add_user();
    (w_handle.clone(), w_handle)
}
