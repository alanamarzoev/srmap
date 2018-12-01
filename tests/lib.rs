extern crate srmap;

fn setup() -> (srmap::srmap::WriteHandle<String, String, Option<i32>>,
               srmap::srmap::WriteHandle<String, String, Option<i32>>)
{
    let uid1: usize = 0 as usize;
    let uid2: usize = 1 as usize;

    let (r, mut w) = srmap::srmap::construct::<String, String, Option<i32>>(None);

    // create two users
    w.add_user(uid1);
    w.add_user(uid2);

    (r, w)
}

#[test]
fn it_works() {
    let k = "x".to_string();
    let v = "x".to_string();
    let v2 = "x2".to_string();
    let v3 = "x3".to_string();

    let (r, mut w) = setup();

    let uid1: usize = 0 as usize;
    let uid2: usize = 1 as usize;

    w.insert(k.clone(), v.clone(), uid1.clone());
    w.insert(k.clone(), v.clone(), uid1.clone());
    w.insert(k.clone(), v.clone(), uid2.clone());
    w.insert(k.clone(), v.clone(), uid2.clone());
    // w.insert(k.clone(), v.clone(), uid2.clone());

    w.insert(k.clone(), v2.clone(), uid2.clone());

    let v_res = r.get_and(&k, |rs| { rs.iter().any(|r| *r == "x".to_string()) }, uid1.clone()).unwrap();

    println!("V: {:?}", v_res.clone());

    let v_ = r.get_and(&k, |rs| { rs.iter().any(|r| *r == "x".to_string()) }, uid2.clone()).unwrap();
    println!("V2: {:?}", v_.clone());

    let v2 = r.get_and(&k, |rs| { rs.iter().any(|r| *r == "x2".to_string()) }, uid2.clone()).unwrap();

    println!("k: {:?} v: {:?} uid {:?}", k.clone(), v.clone(), uid1.clone());
    println!("k: {:?} v: {:?} uid {:?}", k.clone(), v_.clone(), uid2.clone());
    println!("k: {:?} v: {:?} uid {:?}", k.clone(), v2.clone(), uid2.clone());

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
