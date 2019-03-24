#![feature(test)]
#![feature(duration_float)]

extern crate srmap;
extern crate rand;
extern crate time;
extern crate test;
extern crate evmap;

pub use srmap::data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
use test::Bencher;
use std::time::{Duration, Instant};
use std::thread::sleep;

fn setup() -> (srmap::handle::handle::Handle<String, String, Option<i32>>, srmap::handle::handle::Handle<String, String, Option<i32>>)
{
    let (r, mut w) = srmap::construct::<String, String, Option<i32>>(None);
    (r, w)
}

#[test]
fn it_works() {
    let k = "k1".to_string();
    let k2 = "k2".to_string();
    let v = "v1".to_string();
    let v2 = "v2".to_string();
    let v3 = "v3".to_string();

    let (r0, mut w0) = setup(); // global universe
    let (id1, r1, mut w1) =  w0.clone_new_user();
    let (id2, r2, mut w2) =  w0.clone_new_user();

    println!("global insert k: {:?} v: {:?}", k.clone(), v.clone());
    w0.insert(k.clone(), v.clone(), None);
    println!("global insert k: {:?} v: {:?}", k.clone(), v.clone());
    w0.insert(k.clone(), v.clone(), None);
    // let reviewed = w0.meta_get_and(&k, |vals| {
    //     println!("global read... vals: {:#?}", vals);
    // });

    println!("**** user1 insert {:?} {:?}", k.clone(), v.clone());
    w1.insert(k.clone(), v.clone(), None);
    // let reviewed = w1.meta_get_and(&k, |vals| {
    //     println!("user1 read... vals: {:#?}", vals);
    // });

    println!("**** user2 insert {:?} {:?}", k.clone(), v.clone());
    w2.insert(k.clone(), v.clone(), None);

    println!("**** user2 insert {:?} {:?}", k.clone(), v.clone());
    w2.insert(k.clone(), v.clone(), None);

    println!("**** user1 insert {:?} {:?}", k.clone(), v.clone());
    w1.insert(k.clone(), v.clone(), None);

    // println!("user2 insert k: {:?} v: {:?}", k.clone(), v2.clone());
    // w2.insert(k.clone(), v2.clone(), None);
    //
    // println!("user2 insert k: {:?} v: {:?}", k2.clone(), v3.clone());
    // w2.insert(k2.clone(), v3.clone(), None);

    // let reviewed = w2.meta_get_and(&k, |vals| {
    //     println!("user1 read... k: {} vals: {:#?}", k, vals);
    // });
    //
    // let reviewed = w2.meta_get_and(&k, |vals| {
    //     println!("user1 read... k: {} vals: {:#?}", k2, vals);
    // });
}


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
    use evmap;

    let k : DataType = "x".to_string().into();

    let (r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);

    let num_users = 10;
    let num_posts = 1000;
    let num_private = 0;

    let mut recs = get_posts(num_posts as usize);

    for i in recs.clone() {
        w.insert(k.clone(), i.clone(), None);
    }


    let mut handles = Vec::new();
    let mut ev_handles = Vec::new();

    for i in 0..num_users {
        let (id1, r1, mut w1) =  w.clone_new_user();
        let (ev_r, mut ev_w) = evmap::new();
        for i in recs.clone() {
            w1.insert(k.clone(), i.clone(), None);
            ev_w.insert(k.clone(), i);
        }
        ev_w.refresh();

        handles.push(w1.clone());
        ev_handles.push(ev_r.clone());
    }

    let mut dur2 = std::time::Duration::from_millis(0);

    let mut num_rows = 0;
    let start2 = std::time::Instant::now();
    for handle in &handles {
        let reviewed = handle.meta_get_and(&k, |vals| {
            num_rows += vals.len();
        });
    }
    dur2 += start2.elapsed();

    println!(
        "Read {} rows in {:?}s ({:.2} GETs/sec)!",
        num_rows,
        dur2,
        (num_rows) as f64 / dur2.as_float_secs(),
    );

    let mut dur = std::time::Duration::from_millis(0);

    let mut num_rows = 0;
    let start = std::time::Instant::now();
    for handle in &ev_handles {
        let reviewed = handle.meta_get_and(&k, |vals| {
            num_rows += vals.len();
        });
    }

    dur += start.elapsed();

    println!(
        "Read {} rows in {:?}s ({:.2} GETs/sec)!",
        num_rows,
        dur,
        (num_rows) as f64 / dur.as_float_secs(),
    );
}


#[bench]
fn bench_memory_usage(b: &mut Bencher) {
    use time::{Duration, PreciseTime};
    use rand;
    use rand::Rng;
    use evmap;

    let k : DataType = "x".to_string().into();

    let (r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);

    let num_users = 1000;
    let num_posts = 1000;
    let num_private = 0;

    let mut recs = get_posts(num_posts as usize);

    for i in recs.clone() {
        w.insert(k.clone(), i.clone(), None);
    }

    let mut handles = Vec::new();

    for i in 0..num_users {
        let (id1, r1, mut w1) =  w.clone_new_user();
        for i in recs.clone() {
            w1.insert(k.clone(), i.clone(), None);
        }
        handles.push(w1.clone());
    }
}

// #[bench]
// fn bench_insert_throughput(b: &mut Bencher) {
//     let (_r, mut w) = srmap::construct::<String, String, Option<i32>>(None);
//
//     let k = "x".to_string();
//     let v = "x".to_string();
//
//     b.iter(|| {
//         w.insert(k.clone(), v.clone(), 0);
//     });
// }

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
