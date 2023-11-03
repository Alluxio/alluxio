#[no_mangle]
pub extern "C" fn add(a: i32, b: i32) -> i32 {
    a.wrapping_add(b)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn add_one_and_one() {
        assert_eq!(2, add(1, 1));
    }
}