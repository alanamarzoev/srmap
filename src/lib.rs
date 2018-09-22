use std::collections::HashMap;

#[derive(Clone)]
struct SRMap<K,V> {
    g_map: HashMap<K,V>, // Global map
    b_map: HashMap<K,Vec(bool)>, // Auxiliary bit map for global map
    u_map: HashMap<String,V> // Universe specific map (used only when K,V conflict with g_map)
}

impl SRMap<K,V> {
    pub fn new() -> Self {
        SRMap{
            g_map: HashMap<K,V>::new(),
            b_map: HashMap<K,Vec(bool)>::new()),
            u_map: HashMap<String,V>::new()
        }
    }

    pub fn insert(k: K, v: V, uid: u_size){
        // check if record is in the global map
        match self.g_map.get(&k) {
            Some(&val) => {
                // if it *is* in the global map, and the values match, update access for this user
                if val == v {
                    // update flag in global bit map for this user
                    match self.b_map.get(&k) {
                        Some(&bitmap) => {
                            bitmap[uid] = true;
                        },
                        None => {}
                    }
                } else {
                // if v is different, insert (k,v) into umap as ('uid:k',v)
                    let uid_str: String =  String::from(uid).to_owned();
                    let k_str: String = String::from(k).to_owned();
                    let u_key = format!("{}{}", uid_str, k_str);
                    self.u_map.insert(u_key.clone(), v.clone());
                }
            },
            // add record to global map if it isn't already there
            None => {
                self.g_map.insert(k.clone(), v.clone());
            }
        }
    }

    pub fn get(k: K, uid: u_size) -> Option<V> {
        let uid_str: String =  String::from(uid).to_owned();
        let k_str: String = String::from(k).to_owned();
        let first_check = format!("{}{}", uid_str, k_str);

        match self.u_map.get(&first_check) {
           Some(&val) => {Some(val)},
           _ => {match self.g_map.get(&k) {
                    Some(&g_val) => {
                        match b_map.get(&k) {
                            Some(&bitmap) => {
                                let accessible = bitmap[uid];
                                if accessible {
                                    return g_val;
                                }
                                else {
                                    return None;
                                }
                            },
                            None => {
                                None
                            }
                        }
                    },
                    _ => {
                        None
                    }
                 }
             }
         }
    }

    pub fn remove(k: K, v:V, uid: u_size) {
        let uid_str: String =  String::from(uid).to_owned();
        let k_str: String = String::from(k).to_owned();
        let first_check = format!("{}{}", uid_str, k_str);

        match self.u_map.get(&first_check) {
            // if umap contains user specific key, just remove that
            Some(&val) => {self.u_map.remove(first_check)},
            // if no user specific version exists, remove user privileges for key from gmap.
            // if no users have access to a key anymore, remove it entirely.
            None => {
                match self.b_map(&k){
                    Some(&bitmap) => {
                        bitmap[uid] = false;
                        let mut remove_entirely = true;
                        for pt in &bitmap {
                            if pt {
                                remove_entirely = false;
                            }
                        }
                        if remove_entirely {
                            self.g_map.remove(&k);
                            self.b_map.remove(&k);
                        }
                    }
                    None => {}
                }
            }
        }
    }

    pub fn add_user(uid: u_size) {
        // add bitmap flag for this user in every global bitmap
        for (k, bmap) in &self.b_map {
            let new_bmap = bmap.append(false); // TODO can you modify bmap in place?
            self.b_map.insert(k, new_bmap);
        }
    }

    pub fn remove_user(k: K, uid: u_size) {
        // remove all u_map records for this user and revoke access from all global entries
        for (k, bmap) in &self.b_map {
            bmap[uid] = false;
        }
    }

}
