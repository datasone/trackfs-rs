use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    io::SeekFrom,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use dashmap::DashMap;
use flac_process::*;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use futures::stream::StreamExt;
use libc::{EINVAL, EIO, ENOENT, ENOTDIR};
use num_rational::Rational32;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::RwLock,
};

use crate::libflac_wrapper::{FlacDecoder, FlacEncoder};

mod flac_process;
mod libc_wrappers;

/// As there are embedded cue in flac, it represents either cue or flac (with
/// cue embedded) path
type CUEPath = PathBuf;
type TrackID = usize;

#[derive(Clone, Default, Debug)]
struct VirtualFSEntry {
    #[allow(dead_code)]
    parent_inode: u64,
    virtual_path: PathBuf,
    origin:       VirtualFSEntryOrigin,
}

#[derive(Clone, Debug)]
enum VirtualFSEntryOrigin {
    Directory(PathBuf),
    Symlink(PathBuf),
    Passthrough(PathBuf),
    CUEVirtualFile(CUEPath, TrackID),
}

impl Default for VirtualFSEntryOrigin {
    fn default() -> Self {
        Self::Directory(PathBuf::new())
    }
}

struct CUEInfoCache {
    info:  CUEInfo,
    mtime: i64,
}

#[derive(Clone, Debug)]
struct DirEntry {
    inode:     u64,
    file_type: FileType,
    name:      OsString,
}

struct DirEntryCache {
    entries: Vec<DirEntry>,
    mtime:   i64,
}

pub struct TrackFS {
    handle:    tokio::runtime::Handle,
    inner:     Arc<TrackFSInner>,
    separator: char,
}

struct TrackFSInner {
    inode_table:    RwLock<Vec<VirtualFSEntry>>,
    inode_lookup:   DashMap<PathBuf, u64>,
    cue_info_cache: DashMap<CUEPath, CUEInfoCache>,
    childs_cache:   DashMap<u64, DirEntryCache>,
    frames_cache:   concurrent_lru::sharded::LruCache<CUEPath, FlacCacheData>,
    libflac_pool:   deadpool::unmanaged::Pool<LibFlacDecEnc>,
}

#[derive(thiserror::Error, Debug)]
pub enum GetCUEError {
    #[error("IO error")]
    IOError(#[from] tokio::io::Error),
    #[error("no embedded cue")]
    NoEmbeddedCUEInFlac,
    #[error("invalid extension")]
    InvalidExtension,
    #[error("")]
    Other(#[from] anyhow::Error),
}

impl TrackFSInner {
    async fn get_cue_info(&self, cue_path: impl AsRef<Path>) -> Result<CUEInfo, GetCUEError> {
        let cue_path_key = cue_path.as_ref().to_path_buf();
        let mtime = tokio::fs::metadata(cue_path.as_ref()).await?.mtime();

        if let Some(info) = self.cue_info_cache.get(&cue_path_key) {
            if info.mtime >= mtime {
                return Ok(info.info.clone());
            }
        }

        let info = match cue_path.as_ref().extension() {
            Some(cue) if cue == OsStr::new("cue") => process_cue(cue_path).await?,
            Some(flac) if flac == OsStr::new("flac") => process_flac_embedded_cue(cue_path)
                .await?
                .ok_or(GetCUEError::NoEmbeddedCUEInFlac)?,
            _ => Err(GetCUEError::InvalidExtension)?,
        };

        self.cue_info_cache
            .insert(cue_path_key.clone(), CUEInfoCache { info, mtime });
        let info = self.cue_info_cache.get(&cue_path_key).unwrap().info.clone();
        Ok(info)
    }

    async fn update_dir_entries(
        &self,
        separator: char,
        parent_inode: u64,
        path: impl AsRef<Path>,
        real_path: impl AsRef<Path>,
    ) -> Result<Vec<DirEntry>, libc::c_int> {
        let path = path.as_ref().to_path_buf();
        let mtime = path.metadata().map(|metadata| metadata.mtime()).ok();
        if let Some(cache) = self.childs_cache.get(&parent_inode) {
            if mtime.unwrap_or(i64::MAX) < cache.mtime {
                return Ok(cache.entries.clone());
            }
        }

        let childs: tokio::io::Result<Vec<PathBuf>> = try {
            let mut childs = vec![];
            let mut read_dir = tokio::fs::read_dir(real_path.as_ref()).await?;
            while let Some(entry) = read_dir.next_entry().await? {
                childs.push(entry.path());
            }
            childs
        };
        let Ok(childs) = childs else {
            return Err(EIO);
        };

        let mut entries = Vec::new();

        let files = childs.iter().filter(|e| e.is_file());
        let dirs = childs.iter().filter(|e| e.is_dir());
        let symlinks = childs.iter().filter(|e| e.is_symlink());

        for dir in dirs {
            let name = dir.file_name().unwrap();
            let mut virtual_path = path.clone();
            virtual_path.push(name);

            let inode = self
                .add_or_update_entry(VirtualFSEntry {
                    parent_inode,
                    virtual_path,
                    origin: VirtualFSEntryOrigin::Directory(dir.clone()),
                })
                .await;

            let entry = DirEntry {
                inode,
                file_type: FileType::Directory,
                name: name.to_os_string(),
            };
            entries.push(entry);
        }

        for symlink in symlinks {
            let name = symlink.file_name().unwrap();
            let mut virtual_path = path.clone();
            virtual_path.push(name);

            let inode = self
                .add_or_update_entry(VirtualFSEntry {
                    parent_inode,
                    virtual_path,
                    origin: VirtualFSEntryOrigin::Symlink(symlink.clone()),
                })
                .await;

            let entry = DirEntry {
                inode,
                file_type: FileType::Symlink,
                name: name.to_os_string(),
            };
            entries.push(entry);
        }

        let cue_files = files
            .clone()
            .filter(|path| path.extension() == Some(OsStr::new("cue")))
            .cloned()
            .collect::<HashSet<_>>();
        let flac_files = files
            .clone()
            .filter(|path| path.extension() == Some(OsStr::new("flac")))
            .cloned()
            .collect::<HashSet<_>>();

        let mut cue_infos: Vec<_> = futures::stream::iter(cue_files.iter())
            .filter_map(|cue| {
                let path = real_path.as_ref().to_path_buf();
                async move {
                    let mut cue_path = path.clone();
                    cue_path.push(cue);

                    match self.get_cue_info(&cue_path).await {
                        Ok(cue) => Some(cue),
                        Err(e) => {
                            tracing::error!("Error parsing cue {}: {e:?}", cue_path.display());
                            None
                        }
                    }
                }
            })
            .collect()
            .await;

        let used_flacs = cue_infos
            .iter()
            .flat_map(|info| {
                info.passthrough_files
                    .iter()
                    .map(|file_info| &file_info.file_path)
                    .chain(info.files_info.iter().map(|file_info| &file_info.file_path))
            })
            .cloned()
            .collect::<HashSet<_>>();
        let left_flacs = &flac_files - &used_flacs;
        let left_cue_infos: Vec<_> = futures::stream::iter(left_flacs.iter())
            .filter_map(|flac| {
                let path = real_path.as_ref().to_path_buf();
                async move {
                    let mut flac_path = path.clone();
                    flac_path.push(flac);

                    match self.get_cue_info(flac).await {
                        Ok(embedded_cue) => Some(embedded_cue),
                        Err(GetCUEError::NoEmbeddedCUEInFlac) => None,
                        Err(e) => {
                            tracing::error!(
                                "Error parsing embedded cue in {}: {e:?}",
                                flac_path.display()
                            );
                            None
                        }
                    }
                }
            })
            .collect()
            .await;
        let flacs_with_cue = left_cue_infos
            .iter()
            .map(|cue_info| &cue_info.cue_name)
            .cloned()
            .collect::<HashSet<_>>();
        cue_infos.extend(left_cue_infos);
        for cue_info in cue_infos {
            for passthrough_info in cue_info.passthrough_files.iter() {
                let passthrough_file = &passthrough_info.file_path;
                let mut vfs_name = cue_info.cue_name.file_name().unwrap().to_os_string();
                // Passthrough files in `CUEInfo` refers to the files with only one track in
                // it
                let track_id = passthrough_info.tracks_info[0].track_id;
                vfs_name.push(format!(
                    "_{}tr{}_{}.flac",
                    separator,
                    track_id + 1,
                    passthrough_info.cue.tracks[track_id].1.title
                ));

                let mut virtual_path = path.clone();
                virtual_path.push(&vfs_name);

                let inode = self
                    .add_or_update_entry(VirtualFSEntry {
                        parent_inode,
                        virtual_path,
                        origin: VirtualFSEntryOrigin::Passthrough(passthrough_file.clone()),
                    })
                    .await;

                let entry = DirEntry {
                    inode,
                    file_type: FileType::RegularFile,
                    name: vfs_name,
                };
                entries.push(entry);
            }

            for file_info in cue_info.files_info.iter() {
                let vfs_name = cue_info.cue_name.file_name().unwrap().to_os_string();
                for track_info in &file_info.tracks_info {
                    let mut vfs_name = vfs_name.clone();
                    let track_id = track_info.track_id;
                    vfs_name.push(format!(
                        "_{}tr{}_{}.flac",
                        separator,
                        track_id + 1,
                        file_info.cue.tracks[track_id].1.title
                    ));

                    let mut virtual_path = path.clone();
                    virtual_path.push(&vfs_name);

                    let inode = self
                        .add_or_update_entry(VirtualFSEntry {
                            parent_inode,
                            virtual_path,
                            origin: VirtualFSEntryOrigin::CUEVirtualFile(
                                cue_info.cue_name.clone(),
                                track_id,
                            ),
                        })
                        .await;

                    let entry = DirEntry {
                        inode,
                        file_type: FileType::RegularFile,
                        name: vfs_name,
                    };
                    entries.push(entry);
                }
            }
        }

        let files = files.cloned().collect::<HashSet<_>>();
        let left_files = &files - &used_flacs;
        let left_files = &left_files - &flacs_with_cue;
        let left_files = &left_files - &cue_files;
        for left_file in left_files {
            let name = left_file.file_name().unwrap();
            let mut virtual_path = path.clone();
            virtual_path.push(name);

            let inode = self
                .add_or_update_entry(VirtualFSEntry {
                    parent_inode,
                    virtual_path,
                    origin: VirtualFSEntryOrigin::Passthrough(left_file.clone()),
                })
                .await;

            let entry = DirEntry {
                inode,
                file_type: FileType::RegularFile,
                name: name.to_os_string(),
            };
            entries.push(entry);
        }

        self.childs_cache.insert(parent_inode, DirEntryCache {
            entries: entries.clone(),
            mtime:   mtime.unwrap_or(0),
        });

        Ok(entries)
    }

    pub async fn get_file_attr(&self, ino: u64) -> Result<FileAttr, libc::c_int> {
        let guard = self.inode_table.read().await;
        let Some(entry) = guard.get(ino as usize) else {
            return Err(ENOENT);
        };

        // Here we use a naive estimation (just use raw stream size, which is definitely
        // larger than cut flac), as the file size must overestimate, otherwise
        // applications may not access the full file.
        let (origin_path, estimated_size) = match entry.origin {
            VirtualFSEntryOrigin::Directory(ref path)
            | VirtualFSEntryOrigin::Symlink(ref path)
            | VirtualFSEntryOrigin::Passthrough(ref path) => (path.clone(), None),

            VirtualFSEntryOrigin::CUEVirtualFile(ref cue_path, track_id) => {
                match self.get_cue_info(cue_path).await {
                    Ok(info) => {
                        let Some(file_info) = info
                            .files_info
                            .iter()
                            .chain(info.passthrough_files.iter())
                            .find(|info| {
                                info.tracks_info
                                    .iter()
                                    .any(|info| info.track_id == track_id)
                            })
                        else {
                            tracing::error!(
                                "Error processing {}: Invalid track id {track_id}",
                                cue_path.display()
                            );
                            return Err(ENOENT);
                        };

                        let Some((_, track_pos)) = file_info.cue.tracks[track_id]
                            .1
                            .indices
                            .iter()
                            .find(|index| index.0 == 1)
                        else {
                            tracing::error!(
                                "Invalid cue: index error for track {track_id} in {}",
                                cue_path.display()
                            );
                            return Err(ENOENT);
                        };

                        let sample_rate = Rational32::from_integer(file_info.sample_rate as i32);
                        let track_pos =
                            (Rational32::from(*track_pos) * sample_rate).to_integer() as u64;
                        let next_track_pos =
                            file_info.cue.tracks.get(track_id + 1).and_then(|track| {
                                track.1.indices.iter().find(|index| index.0 == 1)
                            });

                        let sample_length = match next_track_pos {
                            Some(next_track_pos) => {
                                let next_track_pos =
                                    (Rational32::from(next_track_pos.1) * sample_rate).to_integer()
                                        as u64;
                                next_track_pos - track_pos
                            }
                            None => file_info.total_samples - track_pos,
                        };
                        let sample_length = sample_length
                            * file_info.channels as u64
                            * file_info.bits_per_sample as u64
                            / 8;

                        (file_info.file_path.clone(), Some(sample_length))
                    }
                    Err(e) => {
                        tracing::error!("Error processing {}: {e:?}", cue_path.display());
                        return Err(ENOENT);
                    }
                }
            }
        };

        match libc_wrappers::lstat(origin_path.as_os_str()) {
            Ok(stat) => {
                let mut attr = TrackFS::stat_to_fuse(stat, ino);
                if let Some(estimated_size) = estimated_size {
                    TrackFS::set_attr_size(&mut attr, estimated_size);
                }
                Ok(attr)
            }
            Err(code) => Err(code),
        }
    }

    fn lookup_inode(&self, path: impl AsRef<Path>) -> Option<u64> {
        Some(*self.inode_lookup.get(path.as_ref())?)
    }

    async fn add_or_update_entry(&self, entry: VirtualFSEntry) -> u64 {
        let path = entry.virtual_path.clone();
        let inode_found = self
            .inode_lookup
            .get(&entry.virtual_path)
            .map(|inode| *inode);

        match inode_found {
            Some(inode) => {
                *self
                    .inode_table
                    .write()
                    .await
                    .get_mut(inode as usize)
                    .unwrap() = entry;
                self.inode_lookup.insert(path, inode);
                inode
            }
            None => {
                self.inode_table.write().await.push(entry);
                let inode = (self.inode_table.read().await.len() - 1) as u64;
                self.inode_lookup.insert(path, inode);
                inode
            }
        }
    }
}

struct LibFlacDecEnc {
    decoder: FlacDecoder,
    encoder: FlacEncoder,
}

impl LibFlacDecEnc {
    fn new() -> Self {
        Self {
            decoder: FlacDecoder::new(),
            encoder: FlacEncoder::new(),
        }
    }

    fn split(&mut self) -> (&mut FlacDecoder, &mut FlacEncoder) {
        (&mut self.decoder, &mut self.encoder)
    }
}

const TTL: Duration = Duration::from_secs(30);

impl TrackFS {
    pub async fn new(
        cap: usize,
        root_dir: impl AsRef<Path>,
        separator: char,
        handle: tokio::runtime::Handle,
    ) -> Self {
        let libflac_resources = (0..num_cpus::get())
            .map(|_| LibFlacDecEnc::new())
            .collect::<Vec<_>>();
        let inner = TrackFSInner {
            // We need a dummy one, as the fuse inode start with 1
            inode_table:    RwLock::new(vec![VirtualFSEntry::default()]),
            inode_lookup:   DashMap::new(),
            cue_info_cache: DashMap::new(),
            childs_cache:   DashMap::new(),
            frames_cache:   concurrent_lru::sharded::LruCache::new(cap as u64),
            libflac_pool:   deadpool::unmanaged::Pool::from(libflac_resources),
        };
        let new_self = Self {
            handle,
            inner: Arc::new(inner),
            separator,
        };

        let root_entry = VirtualFSEntry {
            parent_inode: 0,
            virtual_path: PathBuf::from("/"),
            origin:       VirtualFSEntryOrigin::Directory(root_dir.as_ref().to_path_buf()),
        };

        new_self.inner.add_or_update_entry(root_entry).await;
        new_self
    }

    fn mode_to_filetype(mode: libc::mode_t) -> FileType {
        match mode & libc::S_IFMT {
            libc::S_IFDIR => FileType::Directory,
            libc::S_IFREG => FileType::RegularFile,
            libc::S_IFLNK => FileType::Symlink,
            libc::S_IFBLK => FileType::BlockDevice,
            libc::S_IFCHR => FileType::CharDevice,
            libc::S_IFIFO => FileType::NamedPipe,
            libc::S_IFSOCK => FileType::Socket,
            _ => {
                panic!("unknown file type");
            }
        }
    }

    fn stat_to_fuse(stat: libc::stat64, ino: u64) -> FileAttr {
        // st_mode encodes both the kind and the permissions
        let kind = Self::mode_to_filetype(stat.st_mode);
        let perm = (stat.st_mode & 0o7777) as u16;

        let time = |secs: i64, nanos: i64| {
            SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nanos as u32)
        };

        // libc::nlink_t is wildly different sizes on different platforms:
        // linux amd64: u64
        // linux x86:   u32
        // macOS amd64: u16
        #[allow(clippy::cast_lossless)]
        let nlink = stat.st_nlink as u32;

        FileAttr {
            ino,
            size: stat.st_size as u64,
            blocks: stat.st_blocks as u64,
            atime: time(stat.st_atime, stat.st_atime_nsec),
            mtime: time(stat.st_mtime, stat.st_mtime_nsec),
            ctime: time(stat.st_ctime, stat.st_ctime_nsec),
            crtime: SystemTime::UNIX_EPOCH,
            kind,
            perm,
            nlink,
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u32,
            blksize: stat.st_blksize as u32,
            flags: 0,
        }
    }

    fn set_attr_size(attr: &mut FileAttr, new_size: u64) {
        attr.size = new_size;
        attr.blocks = new_size / attr.blksize as u64;
    }
}

enum TrackFSFileHandle {
    TokioFile(tokio::io::BufReader<tokio::fs::File>),
    InMemory(Vec<u8>),
}

// We assume that getting with inode will always in cache
impl Filesystem for TrackFS {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut KernelConfig,
    ) -> Result<(), std::ffi::c_int> {
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_owned();
        let inner = self.inner.clone();
        let separator = self.separator;
        self.handle.spawn(async move {
            let guard = inner.inode_table.read().await;
            let Some(parent_entry) = guard.get(parent as usize) else {
                reply.error(ENOENT);
                return;
            };
            let VirtualFSEntryOrigin::Directory(ref origin_path) = parent_entry.origin else {
                reply.error(ENOTDIR);
                return;
            };

            let mut path = parent_entry.virtual_path.clone();
            let origin_path = origin_path.clone();
            drop(guard);
            if !inner.childs_cache.contains_key(&parent) {
                let _ = inner
                    .update_dir_entries(separator, parent, path.as_path(), origin_path)
                    .await;
            }

            path.push(name);
            let Some(inode) = inner.lookup_inode(path) else {
                reply.error(ENOENT);
                return;
            };

            match inner.get_file_attr(inode).await {
                Ok(attr) => reply.entry(&TTL, &attr, 0),
                Err(code) => reply.error(code),
            }
        });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            match inner.get_file_attr(ino).await {
                Ok(attr) => reply.attr(&TTL, &attr),
                Err(code) => reply.error(code),
            }
        });
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            // It could be much more complex for symlinks handling, but we only use a simple
            // one now
            let guard = inner.inode_table.read().await;
            let Some(entry) = guard.get(ino as usize) else {
                reply.error(ENOENT);
                return;
            };
            if let VirtualFSEntryOrigin::Symlink(ref origin) = entry.origin {
                match std::fs::read_link(origin) {
                    Ok(target) => reply.data(target.into_os_string().as_bytes()),
                    Err(_) => reply.error(EIO),
                }
            } else {
                reply.error(ENOENT);
            }
        });
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let handle = self.handle.clone();
        let inner = self.inner.clone();

        // libflac is a blocking library
        self.handle.spawn(async move {
            let guard = inner.inode_table.read().await;
            let Some(entry) = guard.get(ino as usize) else {
                reply.error(ENOENT);
                return;
            };
            let entry = entry.clone();
            drop(guard);

            let handle = match entry.origin {
                VirtualFSEntryOrigin::Passthrough(origin) => {
                    let Ok(file) = tokio::fs::File::open(origin).await else {
                        reply.error(EIO);
                        return;
                    };
                    let reader = tokio::io::BufReader::new(file);
                    TrackFSFileHandle::TokioFile(reader)
                }
                VirtualFSEntryOrigin::CUEVirtualFile(cue_path, track_id) => {
                    let inner = inner.clone();
                    let cue_path_async = cue_path.clone();
                    let result = handle
                        .spawn_blocking(move || async move {
                            let mut libflac_tools = inner.libflac_pool.get().await.unwrap();

                            let result: anyhow::Result<Vec<u8>> = try {
                                let cue_info = inner.get_cue_info(&cue_path_async).await?;
                                let cache_data = inner.frames_cache.get(cue_path_async.clone());
                                let cache_data = cache_data.as_ref().map(|handle| handle.value());
                                let file_info = cue_info
                                    .files_info
                                    .iter()
                                    .find(|file_info| {
                                        file_info
                                            .tracks_info
                                            .iter()
                                            .any(|info| info.track_id == track_id)
                                    })
                                    .ok_or_else(|| anyhow::anyhow!("Invalid track id"))?;

                                let mut out_bytes = std::io::Cursor::new(Vec::<u8>::new());
                                if let Some(cache_data) = process_file(
                                    &mut libflac_tools,
                                    &mut out_bytes,
                                    &file_info.file_path,
                                    track_id,
                                    &file_info.cue,
                                    &file_info.tracks_info,
                                    cache_data,
                                )
                                .await?
                                {
                                    inner.frames_cache.advice_evict(cue_path_async.clone());
                                    inner
                                        .frames_cache
                                        .get_or_init(cue_path_async, 1, |_| cache_data);
                                }

                                out_bytes.into_inner()
                            };

                            result
                        })
                        .await
                        .unwrap()
                        .await;

                    match result {
                        Ok(bytes) => TrackFSFileHandle::InMemory(bytes),
                        Err(e) => {
                            tracing::error!(
                                "Error while processing {} track {track_id}: {e:?}",
                                cue_path.display()
                            );
                            reply.error(EINVAL);
                            return;
                        }
                    }
                }
                _ => {
                    reply.error(EINVAL);
                    return;
                }
            };
            let handle = Box::new(handle);
            let handle = Box::leak(handle) as *mut TrackFSFileHandle;
            reply.opened(handle as u64, 0);
        });
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }

        self.handle.spawn(async move {
            let handle = fh as *mut TrackFSFileHandle;
            match unsafe { &mut *handle } {
                TrackFSFileHandle::TokioFile(ref mut reader) => {
                    let mut buf = vec![0; size as usize];

                    if reader.seek(SeekFrom::Start(offset as u64)).await.is_err() {
                        reply.error(EIO);
                        return;
                    }
                    let Ok(read_bytes) = reader.read(&mut buf).await else {
                        reply.error(EIO);
                        return;
                    };
                    reply.data(&buf[..read_bytes]);
                }
                TrackFSFileHandle::InMemory(bytes) => {
                    if offset as usize > bytes.len() {
                        reply.data(&[]);
                    } else {
                        let end = offset as usize + size as usize;
                        let end = std::cmp::min(end, bytes.len());
                        reply.data(&bytes[offset as usize..end]);
                    }
                }
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let handle = fh as *mut TrackFSFileHandle;
        let _ = unsafe { Box::from_raw(handle) };
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let separator = self.separator;
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            let guard = inner.inode_table.read().await;
            let Some(entry) = guard.get(ino as usize) else {
                reply.error(ENOENT);
                return;
            };
            let path = entry.virtual_path.clone();
            let origin = entry.origin.clone();
            drop(guard);

            match origin {
                VirtualFSEntryOrigin::Directory(real_path) => {
                    match inner
                        .update_dir_entries(separator, ino, path, real_path)
                        .await
                    {
                        Ok(entries) => {
                            let handle = Box::new(entries);
                            let handle = Box::leak(handle) as *mut Vec<DirEntry>;
                            reply.opened(handle as u64, 0);
                        }
                        Err(code) => {
                            reply.error(code);
                        }
                    }
                }
                _ => reply.error(ENOTDIR),
            }
        });
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = fh as *mut Vec<DirEntry>;

        for (
            i,
            DirEntry {
                inode,
                file_type,
                name,
            },
        ) in unsafe { &(*entries) }
            .iter()
            .skip(offset as usize)
            .enumerate()
        {
            let failed = reply.add(*inode, offset + i as i64 + 1, *file_type, name);
            if failed {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        let fh = fh as *mut Vec<DirEntry>;
        let _ = unsafe { Box::from_raw(fh) };
        reply.ok()
    }
}
