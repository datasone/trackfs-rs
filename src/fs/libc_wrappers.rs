// Libc Wrappers :: Safe wrappers around system calls.
//
// Copyright (c) 2016-2019 by William R. Fraser
//

use std::{
    ffi::{CString, OsStr, OsString},
    io, mem,
    os::unix::ffi::OsStringExt,
};

macro_rules! into_cstring {
    ($path:expr, $syscall:expr) => {
        match CString::new($path.into_vec()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    concat!($syscall, ": path {:?} contains interior NUL byte"),
                    OsString::from_vec(e.into_vec())
                );
                return Err(libc::EINVAL);
            }
        }
    };
}

pub fn lstat(path: &OsStr) -> Result<libc::stat64, libc::c_int> {
    let path_c = into_cstring!(path.to_os_string(), "lstat");

    let mut buf: libc::stat64 = unsafe { mem::zeroed() };
    if -1 == unsafe { libc::lstat64(path_c.as_ptr(), &mut buf) } {
        return Err(io::Error::last_os_error().raw_os_error().unwrap());
    }

    Ok(buf)
}
