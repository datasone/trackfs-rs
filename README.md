# trackfs-rs
Fast FUSE filesystem splitting FLAC with CUE into individual track files

This project is inspired by [andresch/trackfs](https://github.com/andresch/trackfs), made faster by not re-encoding the full FLAC track, as well as wrote in Rust language.

Currently, FLAC with CUE files and FLAC with embedded CUE files (through `cuesheet` vorbis comment) are supported.

## Usage
`trackfs-rs --help`:
```
Usage: trackfs-rs [OPTIONS] <BASE_DIR> <MOUNTPOINT>

Arguments:
  <BASE_DIR>    Base directory being converted into trackfs
  <MOUNTPOINT>  Mountpoint

Options:
  -s, --separator <SEPARATOR>
          The separator character used for differentiating cue filename and track name [default: #]
      --max-cache-entries <MAX_CACHE_ENTRIES>
          Max entries of kept flac frame caches (in memory, for fast flac processing) [default: 100]
      --flac-instances <FLAC_INSTANCES>
          Instances of flac decoders and encoders [default: 4]
  -o, --options <OPTIONS>
          Additional mount options, default mount options for trackfs-rs (besides this argument): `default_permissions, nodev, nosuid, noexec, ro, async, allow_root, auto_unmount`
      --no-auto-unmount
          Some people may want to omit `allow_root` and `auto_unmount` option (as this requires `allow_other` in `fuse.conf`), it can be disabled with this flag. Note: due to limitation in `rust` and `clap`, we cannot give default values to `options`
  -h, --help
          Print help
```
