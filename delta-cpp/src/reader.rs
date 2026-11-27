//! Reads one Delta data file with its deletion vector applied.
//!
//! A split carries the file, the DV descriptor JSON that Spark hands the scan, and the physical
//! schema to read. No table snapshot is needed. Parquet reading, column matching (field id, then
//! name) and type widening are the kernel's; this module adds the DV and the Arrow stream. Which
//! object store a file is read from is decided in [`crate::store`].
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow_select::filter::filter_record_batch;
use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{MetadataColumnSpec, SchemaRef, StructType};
use delta_kernel::{Engine, FileMeta, PredicateRef};
use url::Url;

use crate::store::{self, SharedEngine};

/// A data file as the kernel wants it.
pub struct DataFile {
    pub meta: FileMeta,
}

/// Parses a data file location: an absolute local path, or a URL (`file://`, `s3://`, `s3a://`,
/// `gs://`, `abfss://`, ...). A relative path has no store to read from, so it is rejected.
pub fn parse_location(file_path: &str) -> Result<Url, String> {
    if file_path.starts_with('/') {
        Url::from_file_path(file_path).map_err(|_| format!("not an absolute path: {file_path}"))
    } else {
        Url::parse(file_path).map_err(|e| {
            format!("data file location must be an absolute path or a URL ({e}): {file_path}")
        })
    }
}

/// Resolves the file's size when the caller does not know it (`file_size <= 0`), with one `head`
/// request on its store.
pub fn resolve_file(
    location: Url,
    file_size: i64,
    engine: &SharedEngine,
) -> Result<DataFile, String> {
    let size = if file_size > 0 {
        file_size as u64
    } else {
        engine
            .storage_handler()
            .head(&location)
            .map_err(|e| format!("cannot stat {location}: {e}"))?
            .size
    };
    Ok(DataFile {
        meta: FileMeta {
            location,
            last_modified: 0,
            size,
        },
    })
}

/// Parses the physical schema: Delta schema JSON, as in the table's `schemaString` but with
/// physical column names (column mapping) and field-id metadata where the table has them.
pub fn parse_schema(schema_json: &str) -> Result<SchemaRef, String> {
    if schema_json.is_empty() {
        return Err("physical_schema_json is required".into());
    }
    let schema: StructType =
        serde_json::from_str(schema_json).map_err(|e| format!("bad physical schema: {e}"))?;
    Ok(Arc::new(schema))
}

/// Resolves a DV descriptor JSON to the sorted positions of deleted rows. `None` means no DV.
///
/// `table_root` is a directory URL, needed only for storage type `u` (a UUID file relative to the
/// table). Inline (`i`) and absolute-path (`p`) DVs resolve without it.
///
/// A DV file can live in a different bucket than the data file (an absolute-path DV), so it is read
/// with the engine for its own URL; `data_engine` serves inline DVs, which need no I/O.
pub fn resolve_deleted_rows(
    descriptor_json: &str,
    table_root: &str,
    options: &BTreeMap<String, String>,
    data_engine: &Arc<SharedEngine>,
) -> Result<Option<Vec<u64>>, String> {
    if descriptor_json.is_empty() {
        return Ok(None);
    }
    let descriptor: DeletionVectorDescriptor = serde_json::from_str(descriptor_json)
        .map_err(|e| format!("bad deletion vector descriptor: {e}"))?;
    let root = if table_root.is_empty() {
        let is_table_relative = serde_json::from_str::<serde_json::Value>(descriptor_json)
            .ok()
            .and_then(|v| {
                v.get("storageType")
                    .and_then(|s| s.as_str())
                    .map(|s| s == "u")
            })
            .unwrap_or(false);
        if is_table_relative {
            return Err(
                "deletion vector is table-relative (storageType u) but no table root was given"
                    .into(),
            );
        }
        // Inline and absolute-path DVs never look at the root.
        url::Url::parse("file:///").expect("valid url")
    } else {
        let mut root =
            url::Url::parse(table_root).map_err(|e| format!("bad table root {table_root}: {e}"))?;
        if !root.path().ends_with('/') {
            root.set_path(&format!("{}/", root.path()));
        }
        root
    };
    let engine = match descriptor
        .absolute_path(&root)
        .map_err(|e| format!("bad deletion vector location: {e}"))?
    {
        Some(dv_url) => store::engine_for(&dv_url, options)?,
        None => data_engine.clone(),
    };
    let rows = descriptor
        .row_indexes(engine.storage_handler(), &root)
        .map_err(|e| format!("failed to read deletion vector: {e}"))?;
    // The kernel does not compare what it decoded with what the log says. A mismatch means a
    // truncated or wrong file, and reading on would return rows that should be deleted.
    if rows.len() as i64 != descriptor.cardinality {
        return Err(format!(
            "deletion vector has {} rows but its descriptor says {}",
            rows.len(),
            descriptor.cardinality
        ));
    }
    if !rows.windows(2).all(|w| w[0] < w[1]) {
        return Err("deletion vector positions are not sorted and unique".into());
    }
    Ok(Some(rows))
}

/// Name of the helper column that carries each row's position in its file. Not part of the output.
const ROW_INDEX_COLUMN: &str = "__delta_cpp_row_index";

/// Drops the rows of `batch` whose file position is in `deleted` (sorted, unique), and removes the
/// row-index column. Positions come from the column, not from counting rows: the kernel may skip
/// row groups (predicate pushdown), after which a batch no longer starts where the last one ended.
pub fn drop_deleted(batch: RecordBatch, deleted: &[u64]) -> Result<RecordBatch, ArrowError> {
    let idx = batch.schema().index_of(ROW_INDEX_COLUMN)?;
    let positions = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ArrowError::SchemaError("row index column is not a long".into()))?;
    let n = positions.len();
    let mut keep = vec![true; n];
    let mut any = false;
    if n > 0 {
        // Row indexes ascend within a file; only deleted rows between the first and last matter.
        let (first, last) = (positions.value(0) as u64, positions.value(n - 1) as u64);
        let lo = deleted.partition_point(|d| *d < first);
        let hi = deleted.partition_point(|d| *d <= last);
        for d in &deleted[lo..hi] {
            if let Ok(i) = positions.values().binary_search(&(*d as i64)) {
                keep[i] = false;
                any = true;
            }
        }
    }
    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();
    columns.remove(idx);
    fields.remove(idx);
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)?;
    if any {
        filter_record_batch(&batch, &BooleanArray::from(keep))
    } else {
        Ok(batch)
    }
}

type KernelBatches = delta_kernel::FileDataReadResultIterator;

fn external<E: std::error::Error + Send + Sync + 'static>(e: E) -> ArrowError {
    ArrowError::ExternalError(Box::new(e))
}

/// Streams the live rows of one file, one kernel batch at a time.
struct LiveRows {
    batches: KernelBatches,
    schema: ArrowSchemaRef,
    /// Sorted positions of deleted rows. Empty means the batches carry no row-index column.
    deleted: Vec<u64>,
    /// True when the whole file is read, so every deleted position must exist in it. False once a
    /// predicate may skip row groups: the rows after the last one read are then unknown.
    whole_file: bool,
    /// One past the highest file position seen so far.
    rows_seen: u64,
    done: bool,
}

impl LiveRows {
    fn convert(
        &mut self,
        data: Box<dyn delta_kernel::EngineData>,
    ) -> Result<RecordBatch, ArrowError> {
        let batch = ArrowEngineData::try_from_engine_data(data)
            .map_err(external)
            .map(RecordBatch::from)?;
        if self.deleted.is_empty() {
            return Ok(batch);
        }
        let positions = batch
            .column_by_name(ROW_INDEX_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| ArrowError::SchemaError("kernel returned no row index".into()))?;
        if let Some(last) = positions.len().checked_sub(1) {
            self.rows_seen = self.rows_seen.max(positions.value(last) as u64 + 1);
        }
        drop_deleted(batch, &self.deleted)
    }
}

impl Iterator for LiveRows {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.batches.next() {
            Some(Err(e)) => {
                self.done = true;
                Some(Err(external(e)))
            }
            Some(Ok(data)) => {
                let result = self.convert(data);
                if result.is_err() {
                    self.done = true;
                }
                Some(result)
            }
            None => {
                self.done = true;
                // A DV that points past the end of the file means it belongs to a different
                // version of the file, so the result cannot be trusted.
                match self.deleted.last() {
                    Some(d) if self.whole_file && *d >= self.rows_seen => {
                        Some(Err(ArrowError::ExternalError(
                            format!(
                                "deletion vector references row {d} but the file has {} rows",
                                self.rows_seen
                            )
                            .into(),
                        )))
                    }
                    _ => None,
                }
            }
        }
    }
}

impl RecordBatchReader for LiveRows {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

/// Opens the file on `engine` and returns a reader over its live rows. `predicate`, if given, lets
/// the kernel skip row groups it proves cannot match; it never removes rows inside a group.
pub fn open_with(
    engine: &impl Engine,
    file: &DataFile,
    physical_schema: SchemaRef,
    deleted: Option<&[u64]>,
    predicate: Option<PredicateRef>,
) -> Result<impl RecordBatchReader + Send + 'static, String> {
    let arrow_schema: ArrowSchema = physical_schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e: ArrowError| format!("schema to arrow: {e}"))?;
    let deleted = deleted.map(<[u64]>::to_vec).unwrap_or_default();
    // Ask the kernel for each row's real position in the file, only when there is a DV to apply.
    let read_schema = if deleted.is_empty() {
        physical_schema
    } else {
        Arc::new(
            physical_schema
                .add_metadata_column(ROW_INDEX_COLUMN, MetadataColumnSpec::RowIndex)
                .map_err(|e| format!("cannot add row index column: {e}"))?,
        )
    };
    let whole_file = predicate.is_none();
    let batches = engine
        .parquet_handler()
        .read_parquet_files(std::slice::from_ref(&file.meta), read_schema, predicate)
        .map_err(|e| format!("open parquet: {e}"))?;
    Ok(LiveRows {
        batches,
        schema: Arc::new(arrow_schema),
        deleted,
        whole_file,
        rows_seen: 0,
        done: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use delta_kernel::EngineData;

    /// Batches of `(first_file_position, rows)`; the value in each row equals its file position.
    fn live_rows(batches: Vec<(i64, i64)>, deleted: Vec<u64>, whole_file: bool) -> LiveRows {
        let data: Vec<delta_kernel::KernelResult<Box<dyn EngineData>>> = batches
            .into_iter()
            .map(|(start, n)| {
                let pos = Int64Array::from((start..start + n).collect::<Vec<_>>());
                let col =
                    Int32Array::from((start..start + n).map(|p| p as i32).collect::<Vec<_>>());
                let b = RecordBatch::try_from_iter([
                    ("v", Arc::new(col) as _),
                    (ROW_INDEX_COLUMN, Arc::new(pos) as _),
                ])
                .unwrap();
                Ok(Box::new(ArrowEngineData::new(b)) as Box<dyn EngineData>)
            })
            .collect();
        LiveRows {
            batches: Box::new(data.into_iter()),
            schema: Arc::new(ArrowSchema::empty()),
            deleted,
            whole_file,
            rows_seen: 0,
            done: false,
        }
    }

    #[test]
    fn dv_past_the_end_of_the_file_is_an_error() {
        let results: Vec<_> = live_rows(vec![(0, 4)], vec![1, 9], true).collect();
        assert!(results[0].is_ok());
        let err = results[1].as_ref().unwrap_err().to_string();
        assert!(
            err.contains("references row 9 but the file has 4 rows"),
            "{err}"
        );
        assert_eq!(results.len(), 2, "stream ends after the error");
    }

    #[test]
    fn dv_inside_the_file_ends_cleanly() {
        let results: Vec<_> = live_rows(vec![(0, 4), (4, 4)], vec![3, 4], true).collect();
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn batches_that_do_not_start_at_zero_use_their_real_positions() {
        // Row group 0 (positions 0..4) was skipped; the first batch starts at position 4.
        let out: Vec<_> = live_rows(vec![(4, 4)], vec![1, 5, 7], false)
            .collect::<Result<_, _>>()
            .unwrap();
        let v: Vec<i32> = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(v, [4, 6]);
        assert_eq!(out[0].num_columns(), 1, "row index column is removed");
    }

    #[test]
    fn deleted_rows_in_skipped_groups_are_not_an_error() {
        let results: Vec<_> = live_rows(vec![(4, 4)], vec![1, 20], false).collect();
        assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    }

    /// A file of three row groups of 4 rows each, `v` = 0..12, written to a fresh temp directory.
    fn three_row_groups() -> (std::path::PathBuf, DataFile) {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        let dir = std::env::temp_dir().join(format!("delta-cpp-rg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rg.parquet");
        let col = Int32Array::from((0..12).collect::<Vec<_>>());
        let batch = RecordBatch::try_from_iter([("v", Arc::new(col) as _)]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .build();
        let mut w = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            batch.schema(),
            Some(props),
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let size = std::fs::metadata(&path).unwrap().len();
        let meta = FileMeta {
            location: Url::from_file_path(&path).unwrap(),
            last_modified: 0,
            size,
        };
        (dir, DataFile { meta })
    }

    fn read_v(
        file: &DataFile,
        deleted: &[u64],
        predicate: Option<PredicateRef>,
    ) -> Result<Vec<i32>, ArrowError> {
        let engine = store::engine_for(&file.meta.location, &BTreeMap::new()).unwrap();
        let schema = parse_schema(
            r#"{"type":"struct","fields":[{"name":"v","type":"integer","nullable":true,"metadata":{}}]}"#,
        )
        .unwrap();
        let reader = open_with(&*engine, file, schema, Some(deleted), predicate).unwrap();
        let mut out = vec![];
        for b in reader {
            let b = b?;
            assert_eq!(b.num_columns(), 1, "helper column must not leak");
            out.extend(
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
            );
        }
        Ok(out)
    }

    #[test]
    fn dv_is_applied_by_file_position_when_row_groups_are_skipped() {
        use delta_kernel::expressions::{column_expr, Expression, Predicate};
        let (dir, file) = three_row_groups();
        // Delete one row in each group: positions 1, 5 and 10.
        let deleted = [1u64, 5, 10];
        // No predicate: every group is read.
        assert_eq!(
            read_v(&file, &deleted, None).unwrap(),
            [0, 2, 3, 4, 6, 7, 8, 9, 11]
        );
        // `v >= 4` proves group 0 (values 0..=3) cannot match, so the kernel skips it. The batch
        // then starts at position 4, and the deleted rows must still be 5 and 10.
        let pred: PredicateRef =
            Arc::new(Predicate::ge(column_expr!("v"), Expression::literal(4i32)));
        assert_eq!(
            read_v(&file, &deleted, Some(pred)).unwrap(),
            [4, 6, 7, 8, 9, 11]
        );
        // A DV entry inside a skipped group is not an error when a predicate is in play.
        let pred: PredicateRef =
            Arc::new(Predicate::ge(column_expr!("v"), Expression::literal(8i32)));
        assert_eq!(read_v(&file, &deleted, Some(pred)).unwrap(), [8, 9, 11]);
        std::fs::remove_dir_all(dir).ok();
    }
}
