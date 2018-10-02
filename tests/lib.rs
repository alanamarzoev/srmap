extern crate srmap;

#[test]
fn it_works() {
    let k = "x".to_string();
    let v = 42;
    let v2 = 60;
    let uid1: usize = 0 as usize;
    let uid2: usize = 1 as usize;

    // let mut map = srmap::srmap::SRMap::<String, i32>::new();

    let (r, w) = srmap::srmap::construct::<String, i32>();

    // create two users
    w.add_user(uid1);
    w.add_user(uid2);

    w.insert(k.clone(), v.clone(), uid1.clone());
    // println!("After first insert: {:?}", map);

    w.insert(k.clone(), v.clone(), uid2.clone());
    // println!("After first insert: {:?}", map);

    w.insert(k.clone(), v2.clone(), uid2.clone());
    // println!("After overlapping insert: {:?}", map);

    match r.get(k.clone(), uid2.clone()) {
        Some(res) => println!("result: {}", res),
        None => {}
    }

    w.remove(k.clone(), uid1.clone());
    // println!("After remove: {:?}", map);

    w.remove_user(uid1);
    // println!("After removing u1 {:?}", map);

    w.remove_user(uid2);
    // println!("After removing u2 {:?}", map);
}
