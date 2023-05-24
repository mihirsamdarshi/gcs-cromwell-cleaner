# Google Cloud Storage Cromwell Cleaner

[Cromwell](https://github.com/broadinstitute/cromwell) tends to deposit lots of files that you don't actually need in a
bucket.

This is a simple script that attempts to remove those files.

It's easier to parallelize this in Rust than in Python.

## Running it

Ensure that you have [Rust](https://www.rust-lang.org/) installed.

You also need Google Cloud credentials available in your environment.
See [here](https://cloud.google.com/docs/authentication/getting-started) for more information.

To build the binary, run

```bash
cargo build --release
```

After building it, to run it either use:

```bash
./target/release/gcs-cromwell-cleaner --help
```

or

```bash
cargo run --release -- --help.
```

Note that with the second method, the dashes in the middle are important, they signify to cargo you are passing
arguments to the binary, not to cargo itself.

```text
❯ ./gcs-cromwell-cleaner --help
Deletes extraneous Cromwell files from a specified Google Cloud Storage path

Usage: gcs-cromwell-cleaner [OPTIONS] --bucket <gs:// path>

Options:
  -b, --bucket <gs:// path>  The name of the bucket you want to delete files in
      --dry-run              Dry run, don't actually delete any files
  -h, --help                 Print help
  -V, --version              Print version
```
