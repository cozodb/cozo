use cozorocks::DbStatus;
use lazy_static::lazy_static;

#[no_mangle]
extern "C" fn rusty_cmp(a: &cozorocks::Slice, b: &cozorocks::Slice) -> cozorocks::c_int {
    dbg!(cozorocks::convert_slice_back(a));
    dbg!(cozorocks::convert_slice_back(b));
    cozorocks::c_int(0)
}

lazy_static! {
    static ref RUSTY_COMPARATOR: cozorocks::UniquePtr<cozorocks::RustComparator> = {
        unsafe {
            let f_ptr = rusty_cmp as *const cozorocks::c_void;
            cozorocks::new_rust_comparator("hello", false, f_ptr)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_it() {
        let a = cozorocks::convert_slice(&[1, 2, 3, 4]);
        let b = cozorocks::convert_slice(&[4, 5, 6, 7]);
        assert_eq!(RUSTY_COMPARATOR.Compare(&a, &b), cozorocks::c_int(0));
    }
}
