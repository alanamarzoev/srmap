extern crate srmap;
pub use srmap::data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};


fn setup() -> (srmap::handle::handle::Handle<String, String, Option<i32>>, srmap::handle::handle::Handle<String, String, Option<i32>>)
{
    let (r, mut w) = srmap::construct::<String, String, Option<i32>>(None);
    (r, w)
}

#[test]
fn it_works() {
    let k = "x".to_string();
    let k2 = "x2".to_string();
    let v = "x1".to_string();
    let v2 = "x2".to_string();
    let v3 = "x3".to_string();

    let (r0, mut w0) = setup(); // global universe
    let (r1, mut w1) =  w0.clone_new_user();
    let (r2, mut w2) =  w0.clone_new_user();

    w0.insert(k.clone(), v.clone());
    println!("global insert k: {:?} v: {:?}", k.clone(), v.clone());

    w1.insert(k.clone(), v.clone());
    println!("user1 insert k: {:?} v: {:?}", k.clone(), v.clone());

    w2.insert(k.clone(), v.clone());
    println!("user2 insert k: {:?} v: {:?}", k.clone(), v.clone());

    w2.insert(k.clone(), v2.clone());
    println!("user2 insert k: {:?} v: {:?}", k.clone(), v2.clone());

    w2.insert(k2.clone(), v2.clone());
    println!("user2 insert k: {:?} v: {:?}", k2.clone(), v2.clone());

    let reviewed = w2.meta_get_and(&k, |vals| {
        println!("reading out");
        for val in vals {
            println!("{}: {}", k.clone(), val.clone());
        }
    });

    // let mut res_vec = Vec::new();
    // println!("here1");
    // w0.get_and(&k, |s| res_vec.push(s.len()));
    // println!("here2");
    // println!("V: {:?}", res_vec.clone());
    //
    // w.insert(k.clone(), v2.clone(), uid2.clone());
    //
    // let v_res = r.get_and(&k, |rs| { rs.iter().any(|r| *r == "x".to_string()) }, uid1.clone()).unwrap();
    //
    // println!("V: {:?}", v_res.clone());
    //
    // let v_ = r.get_and(&k, |rs| { rs.iter().any(|r| *r == "x".to_string()) }, uid2.clone()).unwrap();
    // println!("V2: {:?}", v_.clone());
    //
    // let v2 = r.get_and(&k, |rs| { rs.iter().any(|r| *r == "x2".to_string()) }, uid2.clone()).unwrap();
    //
    // println!("k: {:?} v: {:?} uid {:?}", k.clone(), v.clone(), uid1.clone());
    // println!("k: {:?} v: {:?} uid {:?}", k.clone(), v_.clone(), uid2.clone());
    // println!("k: {:?} v: {:?} uid {:?}", k.clone(), v2.clone(), uid2.clone());

    // w.insert(k.clone(), v3.clone(), uid1.clone());
    // w.insert(k.clone(), v.clone(), uid2.clone());

    // w.insert(k.clone(), v2.clone(), uid2.clone());
    // println!("After overlapping insert: {:?}", lock.read().unwrap());
    //
    //
    // assert_eq!(v, true);
    //
    // let v = r.get_and(&k, |rs| { rs.iter().any(|r| *r == "x2".to_string()) }, uid2.clone()).unwrap();
    // assert_eq!(v, true);
    //
    // w.remove(k.clone(), uid1.clone());
    // println!("After remove: {:?}", lock.read().unwrap());
    //
    // let v = r.get_and(&k, |_| false, uid1.clone());
    // println!("V: {:?}", v);
    // match v {
    //     Some(val) => assert_eq!(val, false),
    //     None => {}
    // };
    //
    // w.remove(k.clone(), uid2.clone());
    // println!("After user specific remove {:?}", lock.read().unwrap());
    //
    // w.remove_user(uid1);
    // println!("After removing u1 {:?}", lock.read().unwrap());
    //
    // w.remove_user(uid2);
    // println!("After removing u2 {:?}", lock.read().unwrap());
}


// #[bench]
// fn bench_insert_throughput(b: &mut Bencher) {
//     let uid1: usize = 0 as usize;
//     let uid2: usize = 1 as usize;
//
//     let (_r, mut w) = srmap::construct::<String, String, Option<i32>>(None);
//
//     // create two users
//     w.add_user(uid1);
//     w.add_user(uid2);
//
//     let k = "x".to_string();
//     let v = "x".to_string();
//
//     b.iter(|| {
//         w.insert(k.clone(), v.clone(), 0);
//     });
// }
//
// use data::DataType;
// fn get_posts(num: usize) -> Vec<Vec<DataType>> {
//     let mut rng = rand::thread_rng();
//     let mut records : Vec<Vec<DataType>> = Vec::new();
//     for i in 0..num {
//         let pid = i.into();
//         let author = (0 as usize).into();
//         let cid = (0 as usize).into();
//         let content : DataType = format!("post #{}", i).into();
//         let private = (0 as usize).into();
//         let anon = 1.into();
//         records.push(vec![pid, cid, author, content, private, anon]);
//     }
//     records
// }
//
//
// fn get_private_posts(num: usize, uid: usize) -> Vec<Vec<DataType>> {
//     let mut rng = rand::thread_rng();
//     let mut records : Vec<Vec<DataType>> = Vec::new();
//     for i in 0..num {
//         let pid = i.into();
//         let author = (uid.clone() as usize).into();
//         let cid = (0 as usize).into();
//         let content : DataType = format!("post #{}", (i + uid)).into();
//         let private = (0 as usize).into();
//         let anon = 1.into();
//         records.push(vec![pid, cid, author, content, private, anon]);
//     }
//     records
// }

// #[bench]
// fn bench_insert_multival(b: &mut Bencher) {
//     use time::{Duration, PreciseTime};
//     use rand;
//     use rand::Rng;
//     pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
//
//     let (_r, mut w) = srmap::construct::<DataType, Vec<DataType>, Option<i32>>(None);
//
//     let num_users = 10;
//     let num_posts = 20000;
//     let num_private = 0;
//
//     // create users
//     let mut j = 0;
//     while j < num_users {
//         w.add_user(j as usize);
//         j += 1;
//     }
//
//     // add records to global map
//     let k : DataType = "x".to_string().into();
//     let mut avg = Duration::nanoseconds(0);
//
//     let mut recs = get_posts(num_posts as usize);
//     for i in recs {
//         let start = PreciseTime::now();
//         w.insert(k.clone(), i, 0 as usize);
//         let delta = start.to(PreciseTime::now());
//         avg = (avg + delta);
//     }
//
//     // update bitmaps for users sharing global values
//     let mut avg = Duration::nanoseconds(0);
//     for j in 1..num_users + 1 {
//         // insert public posts for each user
//         let mut recs = get_posts(num_posts as usize);
//         for i in recs {
//             w.insert(k.clone(), i, j as usize);
//         }
//         // insert private posts for each user
//         let mut recs = get_private_posts(num_private as usize, j as usize);
//         for i in recs {
//             w.insert(k.clone(), i, j as usize);
//         }
//     }
//
//     for j in 0..num_users + 1 {
//         // insert public posts for each user
//         let mut res = w.get_and(&k.clone(), |s| s.len().clone(), j as usize);
//         // println!("USER: {:?}, NUM_RECS: {:?}", j, res.clone());
//     }


    // let num_avg = avg.num_nanoseconds().unwrap() / i;
    // println!("avg time: {:?}", num_avg.clone());

    // not in global map, umap updates
    // let mut i = 0;
    // let mut avg = Duration::nanoseconds(0);
    // while i < 1000 {
    //     let start = PreciseTime::now();
    //     w.insert(k.clone(), format!("{}", i), uid3);
    //     let delta = start.to(PreciseTime::now());
    //     i += 1;
    //     avg = (avg + delta);
    // }
    // let num_avg = avg.num_nanoseconds().unwrap() / i;
    // println!("avg time: {:?}", num_avg.clone());
//}

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
