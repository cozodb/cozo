#[inline(always)]
pub(crate) fn swap_option_result<T, E>(d: Result<Option<T>, E>) -> Option<Result<T, E>> {
    match d {
        Ok(Some(s)) => Some(Ok(s)),
        Ok(None) => None,
        Err(e) => Some(Err(e)),
    }
}

#[inline(always)]
pub(crate) fn swap_result_option<T, E>(d: Option<Result<T, E>>) -> Result<Option<T>, E> {
    match d {
        None => Ok(None),
        Some(Ok(v)) => Ok(Some(v)),
        Some(Err(e)) => Err(e),
    }
}
