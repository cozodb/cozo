use crate::*;

#[allow(improper_ctypes_definitions)]
#[no_mangle]
extern "C" fn test_comparator(a: &[u8], b: &[u8]) -> i8 {
    use std::cmp::Ordering::*;
    let res = a.cmp(b);

    // println!(
    //     "comparator called: {} vs {} => {:?}",
    //     String::from_utf8_lossy(a),
    //     String::from_utf8_lossy(b),
    //     res
    // );

    match res {
        Equal => 0,
        Greater => 1,
        Less => -1,
    }
}

#[test]
fn creation() {
    for optimistic in [true, false] {
        let db = DbBuilder::default()
            .path(&format!("_test_db_{:?}", optimistic))
            .optimistic(optimistic)
            .create_if_missing(true)
            .use_custom_comparator("rusty_cmp_test", test_comparator, false)
            .destroy_on_exit(true)
            .build()
            .unwrap();

        let mut tx = db.transact().disable_wal(true).start();
        tx.set_snapshot();
        tx.put("hello".as_bytes(), "world".as_bytes()).unwrap();
        tx.put("你好".as_bytes(), "世界".as_bytes()).unwrap();
        assert_eq!(
            "world".as_bytes(),
            tx.get("hello".as_bytes(), false).unwrap().unwrap().as_ref()
        );
        assert_eq!(
            "世界".as_bytes(),
            tx.get("你好".as_bytes(), false).unwrap().unwrap().as_ref()
        );
        assert!(tx.get("bye".as_bytes(), false).unwrap().is_none());

        let mut it = tx.iterator().total_order_seek(true).start();
        it.seek_to_start();
        while let Some((k, v)) = it.pair().unwrap() {
            let mut res = String::from_utf8_lossy(k);
            res += ": ";
            res += String::from_utf8_lossy(v);
            dbg!(res);
            it.next();
        }

        tx.commit().unwrap();
    }
}
