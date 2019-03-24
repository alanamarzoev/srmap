#![feature(trivial_bounds)]
#![feature(test)]
#![feature(try_from)]
#![feature(extern_prelude)]

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

// pub mod inner;
pub mod data;
pub mod handle;

use handle::handle::UserHandle;
use handle::handle::GlobalHandle;
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;

pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};


#[derive(Clone)]
pub enum Handle<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug + std::cmp::Eq,
    V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
{
    Global { handle: GlobalHandle<K, V> },
    User { handle: UserHandle<K, V> },
}


// Constructor for read/write handle tuple (global pair)
pub fn construct<K, V>() -> (Handle<K, V>, Handle<K, V>)
where
   K: Eq + Hash + Clone + std::fmt::Debug + std::cmp::Eq,
   V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
{
    let (_gmap_r, mut _gmap_w) = evmap::new();
    let largest_uid = Arc::new(Mutex::new(0 as usize));
    let largest_rid = Arc::new(Mutex::new(0 as usize));
    let mut global = GlobalHandle{ gmap_r: _gmap_r,
                                   gmap_w: Arc::new(Mutex::new(_gmap_w)),
                                   largest_uid: largest_uid,
                                   largest_rid: largest_rid };
    let mut g_handle = Handle::Global{ handle: global };

    (g_handle.clone(), g_handle)
}


pub fn clone_new_user<K, V>(handle: Handle<K, V>) -> (usize, Handle<K, V>, Handle<K, V>)
where
   K: Eq + Hash + Clone + std::fmt::Debug + std::cmp::Eq,
   V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
{
    match handle {
        Handle::Global{ handle } => {
            let mut umap = Arc::new(RwLock::new(HashMap::new()));
            let mut gmap_acl = Arc::new(RwLock::new(HashMap::new()));
            let mut largest = *handle.largest.lock().unwrap() + 1;
            let mut user_handle = UserHandle{ gmap_r: handle.gmap_r.clone(),
                                              gmap_acl: gmap_acl,
                                              umap: umap,
                                              iid: largest};
            let mut new_handle = Handle::User{ handle: user_handle };
            largest += 1;

            (largest.clone(), new_handle.clone(), new_handle)

        },
        Handle::User{ handle } => panic!("must use global handles to create new user handles!"),
    }
}
