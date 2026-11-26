//! Reads a Delta data file with a deletion vector from real object-store protocols, against local
//! emulators. Ignored by default; run them with `delta-cpp/tests/run-cloud-tests.sh`, or set the
//! variables below and `cargo test -p delta-cpp --test cloud -- --ignored`.
//!
//! * S3 (MinIO): `DELTA_CPP_TEST_S3_ENDPOINT` (e.g. `http://127.0.0.1:9000`), with buckets
//!   `delta-test` and `delta-test-2`; credentials `minioadmin` / `minioadmin`.
//! * GCS (fake-gcs-server): `DELTA_CPP_TEST_GCS_ENDPOINT` (e.g. `http://127.0.0.1:4443`), with
//!   buckets `delta-test` and `delta-test-2`.
//! * Azure (Azurite): `DELTA_CPP_TEST_AZURE=1`, with containers `delta-test` and `delta-test-2` in
//!   the emulator account.
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::Int32Array;
use bytes::Bytes;
use delta::ffi::FfiDeltaScanContext;
use delta::store::engine_for;
use delta::{delta_free_arrow_stream, new_delta_file_reader};
use delta_kernel::Engine;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use url::Url;

const DATA_FILE: &str = "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";
const DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
const SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
const LIVE: [i32; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

fn fixture(name: &str) -> Vec<u8> {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../kernel/tests/data/table-with-dv-small/"
    );
    std::fs::read(format!("{dir}{name}")).unwrap()
}

fn logged_descriptor() -> Value {
    let log = String::from_utf8(fixture("_delta_log/00000000000000000001.json")).unwrap();
    log.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find_map(|v| v.get("add").and_then(|a| a.get("deletionVector")).cloned())
        .expect("fixture has a DV")
}

struct Target {
    /// Scheme and bucket of the first bucket, e.g. `s3://delta-test`.
    base: String,
    /// A second bucket, to prove a DV can live somewhere other than its data file.
    other: String,
    props: Vec<(String, String)>,
}

impl Target {
    fn options(&self) -> BTreeMap<String, String> {
        self.props.iter().cloned().collect()
    }

    fn put(&self, url: &str, bytes: Vec<u8>) {
        let url = Url::parse(url).unwrap();
        engine_for(&url, &self.options())
            .unwrap()
            .storage_handler()
            .put(&url, Bytes::from(bytes), true)
            .unwrap_or_else(|e| panic!("upload to {url} failed: {e}"));
    }

    fn ctx(&self, file: &str, root: &str, dv: Option<&Value>, size: i64) -> FfiDeltaScanContext {
        FfiDeltaScanContext {
            table_root: root.to_string(),
            file_path: file.to_string(),
            file_size: size,
            dv_descriptor_json: dv.map(|d| d.to_string()).unwrap_or_default(),
            physical_schema_json: SCHEMA.to_string(),
            props_keys: self.props.iter().map(|(k, _)| k.clone()).collect(),
            props_values: self.props.iter().map(|(_, v)| v.clone()).collect(),
        }
    }
}

fn read_values(ctx: FfiDeltaScanContext) -> Result<Vec<i32>, String> {
    let reader = new_delta_file_reader(ctx)?;
    let ptr = reader.read()?;
    let stream = unsafe { ArrowArrayStreamReader::from_raw(ptr as *mut FFI_ArrowArrayStream) }
        .map_err(|e| e.to_string());
    let out = stream.and_then(|s| {
        let mut values = vec![];
        for batch in s {
            let batch = batch.map_err(|e| e.to_string())?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            values.extend(col.values().iter().copied());
        }
        Ok(values)
    });
    unsafe { delta_free_arrow_stream(ptr) };
    out
}

fn check(target: &Target) {
    let t = target;
    let table = format!("{}/table", t.base);
    // The table lives in one bucket; the same DV is also stored in the other bucket.
    t.put(&format!("{table}/{DATA_FILE}"), fixture(DATA_FILE));
    t.put(&format!("{table}/{DV_FILE}"), fixture(DV_FILE));
    t.put(&format!("{}/dvs/{DV_FILE}", t.other), fixture(DV_FILE));
    // A DV file that packs two DVs: the fixture's, then a second copy of its record, so the second
    // DV sits at a non-zero offset and a whole-file read would not be the same as a ranged one.
    let first = fixture(DV_FILE);
    let second_offset = first.len();
    let mut packed = first.clone();
    packed.extend_from_slice(&first[1..]);
    let packed_url = format!("{table}/packed.bin");
    t.put(&packed_url, packed.clone());

    let data = format!("{table}/{DATA_FILE}");
    let root = format!("{table}/");
    let u = logged_descriptor();

    // Table-relative DV: the DV is found next to the data file.
    assert_eq!(
        read_values(t.ctx(&data, &root, Some(&u), 635)).unwrap(),
        LIVE,
        "u"
    );

    // Size unknown: one head request finds it.
    assert_eq!(
        read_values(t.ctx(&data, &root, Some(&u), 0)).unwrap(),
        LIVE,
        "unknown size"
    );

    // No DV: every row.
    assert_eq!(
        read_values(t.ctx(&data, &root, None, 635)).unwrap(),
        (0..10).collect::<Vec<_>>(),
        "no dv"
    );

    // Absolute-path DV in the same bucket, and in a different bucket than the data file.
    for dv_url in [
        format!("{table}/{DV_FILE}"),
        format!("{}/dvs/{DV_FILE}", t.other),
    ] {
        let mut p = u.clone();
        p["storageType"] = json!("p");
        p["pathOrInlineDv"] = json!(dv_url);
        assert_eq!(
            read_values(t.ctx(&data, "", Some(&p), 635)).unwrap(),
            LIVE,
            "p {dv_url}"
        );
    }

    // The second DV of a packed file, at a non-zero offset.
    let mut at_offset = u.clone();
    at_offset["storageType"] = json!("p");
    at_offset["pathOrInlineDv"] = json!(packed_url);
    at_offset["offset"] = json!(second_offset);
    assert_eq!(
        read_values(t.ctx(&data, "", Some(&at_offset), 635)).unwrap(),
        LIVE,
        "packed, offset {second_offset}"
    );

    // This store really honours a range: asking for the second record returns only its bytes.
    let url = Url::parse(&packed_url).unwrap();
    let engine = engine_for(&url, &t.options()).unwrap();
    let range = second_offset as u64..packed.len() as u64;
    let got = engine
        .storage_handler()
        .read_files(vec![(url, Some(range.clone()))])
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        got.len() as u64,
        range.end - range.start,
        "ranged read returned the wrong length"
    );
    assert!(got.len() < packed.len());

    // A wrong cardinality is still caught.
    let mut bad = u.clone();
    bad["cardinality"] = json!(7);
    let err = read_values(t.ctx(&data, &root, Some(&bad), 635)).unwrap_err();
    assert!(err.contains("descriptor says 7"), "{err}");

    // A file that is not there is an error, not an empty result.
    let missing = format!("{table}/nope.parquet");
    assert!(read_values(t.ctx(&missing, &root, None, 100)).is_err());
}

#[test]
#[ignore = "needs an S3 emulator: set DELTA_CPP_TEST_S3_ENDPOINT"]
fn s3_with_a_dv_in_another_bucket() {
    let endpoint = std::env::var("DELTA_CPP_TEST_S3_ENDPOINT").expect("DELTA_CPP_TEST_S3_ENDPOINT");
    let props = [
        ("aws_endpoint", endpoint.as_str()),
        ("aws_allow_http", "true"),
        ("aws_region", "us-east-1"),
        ("aws_access_key_id", "minioadmin"),
        ("aws_secret_access_key", "minioadmin"),
    ]
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .to_vec();
    check(&Target {
        base: "s3://delta-test".into(),
        other: "s3://delta-test-2".into(),
        props: props.clone(),
    });
    // s3a:// is the same store.
    check(&Target {
        base: "s3a://delta-test".into(),
        other: "s3a://delta-test-2".into(),
        props,
    });
}

#[test]
#[ignore = "needs a GCS emulator: set DELTA_CPP_TEST_GCS_ENDPOINT"]
fn gcs_with_a_dv_in_another_bucket() {
    let endpoint =
        std::env::var("DELTA_CPP_TEST_GCS_ENDPOINT").expect("DELTA_CPP_TEST_GCS_ENDPOINT");
    let props = [
        ("google_base_url", endpoint.as_str()),
        ("google_skip_signature", "true"),
    ]
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .to_vec();
    check(&Target {
        base: "gs://delta-test".into(),
        other: "gs://delta-test-2".into(),
        props,
    });
}

#[test]
#[ignore = "needs Azurite: set DELTA_CPP_TEST_AZURE=1"]
fn azure_with_a_dv_in_another_container() {
    std::env::var("DELTA_CPP_TEST_AZURE").expect("DELTA_CPP_TEST_AZURE");
    // Azurite's well-known account, reached through the emulator endpoint of `object_store`.
    let props = [("azure_storage_use_emulator", "true")]
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .to_vec();
    check(&Target {
        base: "az://delta-test".into(),
        other: "az://delta-test-2".into(),
        props: props.clone(),
    });
    // The form Spark sends for ADLS Gen2: container and account in the host.
    check(&Target {
        base: "abfss://delta-test@devstoreaccount1.dfs.core.windows.net".into(),
        other: "abfss://delta-test-2@devstoreaccount1.dfs.core.windows.net".into(),
        props,
    });
}
