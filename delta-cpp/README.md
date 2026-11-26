# delta-cpp

A cxx bridge that reads one Delta data file, with its deletion vector applied, as an Arrow C
stream. `libdelta` is the Rust half of Quanton's native Delta reader; Velox calls it per split.

A split is a data file, the DV descriptor JSON Spark hands the scan, the table root (for
table-relative DVs), and the physical schema to read as Delta schema JSON.

## Storage

`file_path` and `table_root` are absolute local paths or URLs: `file://`, `s3://`, `s3a://`,
`gs://`, `abfs://`, `abfss://`, `az://`. There is one object store, and one kernel engine, per
bucket (or container) and set of options; all engines share one multi-thread I/O runtime
(`DELTA_CPP_WORKER_THREADS`, default 4 or the core count if smaller).

**Credentials come from the node.** Stores are built with `from_env()`, so with no options the
node's own identity is used: EC2 instance profile, EKS web identity (IRSA) or ECS task role for
S3; application-default credentials or the metadata server for GCS; managed identity or workload
identity for Azure. Nothing needs passing for that.

Two things are not auto-detected: the **S3 region** (`AWS_REGION`, or `aws_region` in the split
options; it defaults to `us-east-1`) and, for a non-AWS endpoint, `aws_endpoint` (+
`aws_allow_http`).

Anything else goes in the split's `props_keys` / `props_values`, in `object_store`'s config-key
syntax (`aws_*`, `google_*`, `azure_*`). Unrecognised keys are ignored. Options may hold secrets
and are never logged. Short-lived credentials vended by a catalog are not handled yet; they need a
refreshable credential provider and are a separate piece of work.

## Tests

`cargo test -p delta-cpp` runs the unit tests against local files and in-memory stores.
`tests/run-cloud-tests.sh` starts S3, GCS and Azure emulators in Docker and reads a DV table,
including DVs packed into one file and a DV in a different bucket, through each of them.
