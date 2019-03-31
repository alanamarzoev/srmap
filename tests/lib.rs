#![feature(test)]
#![feature(duration_float)]

extern crate evmap;
extern crate rand;
extern crate srmap;
extern crate test;
extern crate time;

pub use srmap::data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
use test::Bencher;

fn setup() -> (
    srmap::handle::handle::Handle<String, String, Option<i32>>,
    srmap::handle::handle::Handle<String, String, Option<i32>>,
) {
    let (r, w) = srmap::construct::<String, String, Option<i32>>(None);
    (r, w)
}

#[test]
fn it_works() {
    let k = "k1".to_string();
    let _k2 = "k2".to_string();
    let v = "v1".to_string();
    let _v2 = "v2".to_string();
    let _v3 = "v3".to_string();

    let (_r0, mut w0) = setup(); // global universe
    let (_id1, _r1, mut w1) = w0.clone_new_user();
    let (_id2, _r2, mut w2) = w0.clone_new_user();


    w0.insert(k.clone(), v.clone(), None);
    w0.insert(k.clone(), v.clone(), None);
    let reviewed = w0.meta_get_and(&k, |vals| {
        println!("vals: {:?}", vals);
        assert!(vals.len() == 2);
    });

    w1.insert(k.clone(), v.clone(), None);
    let reviewed = w1.meta_get_and(&k, |vals| {
        println!("vals: {:?}", vals);
        assert!(vals.len() == 1);
    });

    // println!("**** user2 insert {:?} {:?}", k.clone(), v.clone());
    // w2.insert(k.clone(), v.clone(), None);
    //
    // println!("**** user2 insert {:?} {:?}", k.clone(), v.clone());
    // w2.insert(k.clone(), v.clone(), None);
    //
    // println!("**** user1 insert {:?} {:?}", k.clone(), v.clone());
    // w1.insert(k.clone(), v.clone(), None);

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
    let mut records: Vec<Vec<DataType>> = Vec::new();
    for i in 0..num {
        let pid = i.into();
        let author = (0 as usize).into();
        let cid = (0 as usize).into();
        let content: DataType = format!("post #{}", i).into();
        let private = (0 as usize).into();
        let anon = 1.into();
        records.push(vec![pid, cid, author, content, private, anon]);
    }
    records
}

fn _get_private_posts(num: usize, uid: usize) -> Vec<Vec<DataType>> {
    let mut records: Vec<Vec<DataType>> = Vec::new();
    for i in 0..num {
        let pid = i.into();
        let author = (uid.clone() as usize).into();
        let cid = (0 as usize).into();
        let content: DataType = format!("post #{}", (i + uid)).into();
        let private = (0 as usize).into();
        let anon = 1.into();
        records.push(vec![pid, cid, author, content, private, anon]);
    }
    records
}

#[bench]
fn bench_insert_multival(_b: &mut Bencher) {
    let (_r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);

    let num_users = 10;
    let num_posts = 1000000;

    let recs = get_posts(num_posts as usize);

    let start = std::time::Instant::now();
    for (i, r) in recs.iter().enumerate() {
        let k: DataType = format!("x{}", i % 10000).to_string().into();
        w.insert(k.clone(), r.clone(), None);
    }
    println!(
        "Inserted {} global records in {:?} ({:.2} inserts/sec)!",
        recs.len(),
        start.elapsed(),
        recs.len() as f64 / start.elapsed().as_float_secs(),
    );

    let mut handles = Vec::new();

    let start = std::time::Instant::now();
    for i in 0..num_users {
        let (_id1, _r1, mut w1) = w.clone_new_user();

        // make records accessible to half the users
        if i % 2 == 0 {
            for (j, r) in recs.iter().enumerate() {
                let k: DataType = format!("x{}", j % 10000).to_string().into();
                /*if j % 1000 == 0 {
                    println!("u{}, {}", i, j);
                }*/
                w1.insert(k.clone(), r.clone(), None);
            }
        }

        handles.push(w1.clone());
    }
    println!(
        "Inserted {} user universe records in {:?} ({:.2} inserts/sec)!",
        recs.len() * (num_users / 2),
        start.elapsed(),
        (recs.len() * (num_users / 2)) as f64 / start.elapsed().as_float_secs(),
    );

    let start = std::time::Instant::now();
    let mut total_rows = 0;
    let mut total_reads = 0;
    for handle in &handles {
        for j in 0..100 {
            let k: DataType = format!("x{}", j).to_string().into();
            let _res = handle.meta_get_and(&k, |res| {
                total_rows += res.len();
                total_reads += 1;
            });
        }
    }
    println!(
        "Read {} rows in {:?} ({:.2} reads/sec, {:.2} rows/sec)!",
        total_rows,
        start.elapsed(),
        total_reads as f64 / start.elapsed().as_float_secs(),
        total_rows as f64 / start.elapsed().as_float_secs(),
    );
}

#[bench]
fn bench_memory_usage(_b: &mut Bencher) {
    let (_r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);

    let num_users = 5000;
    let num_posts = 100000;

    let recs = get_posts(num_posts as usize);

    let start = std::time::Instant::now();
    for r in &recs {
        w.insert(r[0].clone(), r.clone(), None);
    }
    println!(
        "Inserted {} global records in {:?} ({:.2} inserts/sec)!",
        recs.len(),
        start.elapsed(),
        recs.len() as f64 / start.elapsed().as_float_secs(),
    );

    let mut handles = Vec::new();

    for i in 0..num_users {
        let (_id1, _r1, mut w1) = w.clone_new_user();

        // make records accessible to 1% of the users
        if i % 100 == 0 {
            let start = std::time::Instant::now();
            for r in &recs {
                w1.insert(r[0].clone(), r.clone(), None);
            }
            println!(
                "Inserted {} user universe {} records in {:?} ({:.2} inserts/sec)!",
                recs.len(),
                i,
                start.elapsed(),
                recs.len() as f64 / start.elapsed().as_float_secs(),
            );
        }

        handles.push(w1.clone());
    }

    let start = std::time::Instant::now();
    let mut total_rows = 0;
    let mut total_reads = 0;
    for handle in &handles[990..] {
        for r in recs.iter() {
            let _res = handle.meta_get_and(&r[0], |res| {
                total_rows += res.len();
                total_reads += 1;
            });
        }
    }
    println!(
        "Read {} rows in {:?} ({:.2} reads/sec, {:.2} rows/sec)!",
        total_rows,
        start.elapsed(),
        total_reads as f64 / start.elapsed().as_float_secs(),
        total_rows as f64 / start.elapsed().as_float_secs(),
    );
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
