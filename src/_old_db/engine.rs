#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::tuple::Tuple;
    use std::{fs, thread};

    #[test]
    fn push_get() {
        {
            let engine = Engine::new("_push_get".to_string(), false).unwrap();
            let sess = engine.session().unwrap();
            for i in (-80..-40).step_by(10) {
                let mut ikey = Tuple::with_null_prefix();
                ikey.push_int(i);
                ikey.push_str("pqr");
                println!("in {:?} {:?}", ikey, ikey.data);
                sess.txn.put(true, &sess.perm_cf, &ikey, &ikey).unwrap();
                let out = sess.txn.get(true, &sess.perm_cf, &ikey).unwrap();
                let out = out.as_ref().map(Tuple::new);
                println!("out {:?}", out);
            }
            let it = sess.txn.iterator(true, &sess.perm_cf);
            it.to_first();
            while let Some((key, val)) = unsafe { it.pair() } {
                println!("a: {:?} {:?}", key.as_ref(), val.as_ref());
                println!("v: {:?} {:?}", Tuple::new(key), Tuple::new(val));
                it.next();
            }
        }
        let _ = fs::remove_dir_all("_push_get");
    }

    #[test]
    fn test_create() {
        let p1 = "_test_db_create1";
        let p2 = "_test_db_create2";
        let p3 = "_test_db_create3";
        {
            {
                let engine = Engine::new(p1.to_string(), true);
                assert!(engine.is_ok());
                let engine = Engine::new(p2.to_string(), false);
                assert!(engine.is_ok());
                let engine = Engine::new(p3.to_string(), true);
                assert!(engine.is_ok());
                let engine2 = Engine::new(p1.to_string(), false);
                assert!(engine2.is_err());
                println!("create OK");
            }
            let engine2 = Engine::new(p2.to_string(), false);
            assert!(engine2.is_ok());
            println!("start ok");
            let engine2 = Arc::new(Engine::new(p3.to_string(), false).unwrap());
            {
                for _i in 0..10 {
                    let _sess = engine2.session().unwrap();
                }
                println!("sess OK");
                let handles = engine2.session_handles.lock().unwrap();
                println!("got handles {}", handles.len());
                let cf_ident = &handles.first().unwrap().read().unwrap().cf_ident;
                println!("Opening ok {}", cf_ident);
                let cf = engine2.db.get_cf(cf_ident).unwrap();
                assert!(!cf.is_null());
                println!("Getting CF ok");
            }
            let mut thread_handles = vec![];

            println!("concurrent");
            for i in 0..10 {
                let engine = engine2.clone();
                thread_handles.push(thread::spawn(move || {
                    let mut sess = engine.session().unwrap();
                    println!("In thread {} {}", i, sess.handle.read().unwrap().cf_ident);
                    let gname = format!("abc{}", i);
                    for _ in 0..1000 {
                        sess.push_env().unwrap();
                        sess.define_variable(&gname, &"xyz".into(), true).unwrap();
                        sess.define_variable("pqr", &"xyz".into(), false).unwrap();
                    }
                    if i & 1 == 0 {
                        sess.commit().unwrap();
                    }
                    println!("pqr {:?}", sess.resolve("pqr"));
                    println!("uvw {:?}", sess.resolve("uvw"));
                    println!("aaa {} {:?}", &gname, sess.resolve(&gname));
                    let it = sess.txn.iterator(false, &sess.temp_cf);
                    it.to_first();
                    // for (key, val) in it.iter() {
                    //     println!("a: {:?} {:?}", key.as_ref(), val.as_ref());
                    //     println!("v: {:?}", Tuple::new(key));
                    // }
                    for _ in 0..50 {
                        sess.pop_env().unwrap();
                    }
                    // if let Err(e) = sess.commit() {
                    //     println!("Err {} with {:?}", i, e);
                    // } else {
                    //     println!("OK!!!! {}", i);
                    //     sess.commit().unwrap();
                    //     sess.commit().unwrap();
                    //     println!("OK!!!!!!!! {}", i);
                    // }
                    // sess.commit().unwrap();
                    // sess.commit().unwrap();
                    println!("pqr {:?}", sess.resolve("pqr"));
                    println!("In thread {} end", i);
                }))
            }

            for t in thread_handles {
                t.join().unwrap();
            }
            println!("All OK");
            {
                let handles = engine2.session_handles.lock().unwrap();
                println!(
                    "got handles {:#?}",
                    handles
                        .iter()
                        .map(|h| h.read().unwrap().cf_ident.to_string())
                        .collect::<Vec<_>>()
                );
            }
        }
        let _ = fs::remove_dir_all(p1);
        let _ = fs::remove_dir_all(p2);
        let _ = fs::remove_dir_all(p3);
    }
}
