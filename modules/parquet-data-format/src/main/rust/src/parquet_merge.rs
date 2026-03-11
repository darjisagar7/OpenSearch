use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString, JValue};
use std::fs::File;
use std::error::Error;
use std::any::Any;
use std::sync::Arc;
use std::panic::AssertUnwindSafe;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use arrow::array::{Int64Array, ArrayRef};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use crate::rate_limited_writer::RateLimitedWriter;

use crate::{log_info, log_error};

// Constants
const READER_BATCH_SIZE: usize = 8192;
const WRITER_BATCH_SIZE: usize = 8192;
const ROW_ID_COLUMN_NAME: &str = "___row_id";

// Custom error types
#[derive(Debug)]
pub enum ParquetMergeError {
    EmptyInput,
    InvalidFile(String),
    SchemaReadError(String),
    WriterCreationError(String),
    BatchProcessingError(String),
}

impl std::fmt::Display for ParquetMergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParquetMergeError::EmptyInput => write!(f, "No input files provided"),
            ParquetMergeError::InvalidFile(path) => write!(f, "Invalid file: {}", path),
            ParquetMergeError::SchemaReadError(msg) => write!(f, "Schema read error: {}", msg),
            ParquetMergeError::WriterCreationError(msg) => write!(f, "Writer creation error: {}", msg),
            ParquetMergeError::BatchProcessingError(msg) => write!(f, "Batch processing error: {}", msg),
        }
    }
}

impl Error for ParquetMergeError {}

// Statistics tracking
struct ProcessingStats {
    files_processed: usize,
    total_rows: usize,
    total_batches: usize,
}

// JNI Entry Point - returns RowIdMapping to Java
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    input_files: JObject<'local>,
    output_file: JString<'local>,
) -> JObject<'local> {
    let result = catch_unwind(|| {
        let input_files_vec = convert_java_list_to_vec(&mut env, input_files)
            .map_err(|e| format!("Failed to convert Java list: {}", e))?;

        let output_path: String = env
            .get_string(&output_file)
            .map_err(|e| format!("Failed to get output file string: {}", e))?
            .into();

        log_info!("Starting merge of {} files to {}", input_files_vec.len(), output_path);

        let ((mapping_array, file_offsets, file_sizes), output_file_id) = process_parquet_files(&input_files_vec, &output_path)?;

        log_info!("Merge completed successfully");
        Ok((mapping_array, file_offsets, file_sizes, output_file_id))
    });

    match result {
        Ok(Ok((mapping_array, file_offsets, file_sizes, output_file_id))) => {
            match create_row_id_mapping_object(&mut env, mapping_array, file_offsets, file_sizes, &output_file_id) {
                Ok(obj) => obj,
                Err(e) => {
                    let error_msg = format!("Failed to create RowIdMapping: {}", e);
                    log_error!("{}", error_msg);
                    let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
                    JObject::null()
                }
            }
        }
        Ok(Err(e)) => {
            let error_msg = format!("Error processing Parquet files: {}", e);
            log_error!("{}", error_msg);
            let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
            JObject::null()
        }
        Err(e) => {
            let error_msg = format!("Rust panic occurred: {:?}", e);
            log_error!("{}", error_msg);
            let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
            JObject::null()
        }
    }
}

// Main processing function - returns row ID mappings as (array, offsets)
pub fn process_parquet_files(input_files: &[String], output_path: &str) -> Result<((Vec<i64>, std::collections::HashMap<String, usize>, std::collections::HashMap<String, usize>), String), Box<dyn Error>> {
    validate_input(input_files)?;
    let schema = read_schema_from_file(&input_files[0])?;
    let mut writer = create_writer(output_path, schema.clone())?;
    let (_stats, mapping_data) = process_files(input_files, &schema, &mut writer)?;
    writer.close()
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to close writer: {}", e)))?;
    let output_file_id = std::path::Path::new(output_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(output_path)
        .to_string();
    Ok((mapping_data, output_file_id))
}

// Validation functions
fn validate_input(input_files: &[String]) -> Result<(), Box<dyn Error>> {
    if input_files.is_empty() {
        return Err(Box::new(ParquetMergeError::EmptyInput));
    }

    for path in input_files {
        if !std::path::Path::new(path).exists() {
            return Err(Box::new(ParquetMergeError::InvalidFile(path.clone())));
        }
    }

    Ok(())
}

// Schema reading
fn read_schema_from_file(file_path: &str) -> Result<SchemaRef, Box<dyn Error>> {
    let file = File::open(file_path)
        .map_err(|e| ParquetMergeError::InvalidFile(format!("{}: {}", file_path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| ParquetMergeError::SchemaReadError(format!("Failed to read schema: {}", e)))?;

    Ok(builder.schema().clone())
}

// Writer creation
fn create_writer(output_path: &str, schema: SchemaRef) -> Result<ArrowWriter<RateLimitedWriter<File>>, Box<dyn Error>> {
    let props = WriterProperties::builder()
        .set_write_batch_size(WRITER_BATCH_SIZE)
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let out_file = File::create(output_path)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create output file: {}", e)))?;

    let throttled_writer = RateLimitedWriter::new(out_file, 20.0 * 1024.0 * 1024.0)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create rate limiter: {}", e)))?;

    ArrowWriter::try_new(throttled_writer, schema, Some(props))
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create writer: {}", e)).into())
}

// File processing - builds mapping arrays directly (no intermediate Vec)
// File processing - builds mapping arrays directly, offsets in input_files order
fn process_files(
    input_files: &[String],
    schema: &SchemaRef,
    writer: &mut ArrowWriter<RateLimitedWriter<File>>,
) -> Result<(ProcessingStats, (Vec<i64>, std::collections::HashMap<String, usize>, std::collections::HashMap<String, usize>)), Box<dyn Error>> {
    let mut current_row_id: i64 = 0;
    let mut stats = ProcessingStats {
        files_processed: 0,
        total_rows: 0,
        total_batches: 0,
    };

    // First pass: count rows per file and collect file IDs in input order
    let mut file_ids: Vec<String> = Vec::new();
    let mut file_sizes: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for path in input_files {
        let old_file_id = extract_writer_generation(path)
            .unwrap_or_else(|| {
                std::path::Path::new(path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(path)
                    .to_string()
            });

        let file = File::open(path)
            .map_err(|e| ParquetMergeError::InvalidFile(format!("{}: {}", path, e)))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create reader: {}", e)))?
            .with_batch_size(READER_BATCH_SIZE)
            .build()
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to build reader: {}", e)))?;

        let mut file_row_count = 0;
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to read batch: {}", e)))?;
            file_row_count += batch.num_rows();
        }

        file_ids.push(old_file_id.clone());
        file_sizes.insert(old_file_id, file_row_count);
    }

    // Assign offsets in input_files order (NOT sorted - order must match processing order)
    let mut file_offsets: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut current_offset = 0;
    for file_id in &file_ids {
        file_offsets.insert(file_id.clone(), current_offset);
        // log_info!("File '{}' assigned offset {} (size: {})", file_id, current_offset, file_sizes[file_id]);
        current_offset += file_sizes[file_id];
    }

    // Allocate mapping array
    let total_size = current_offset;
    let mut mapping_array = vec![0i64; total_size];

    // Second pass: process files and build mapping
    current_row_id = 0;
    for (idx, path) in input_files.iter().enumerate() {
        let old_file_id = &file_ids[idx];

        let file = File::open(path)
            .map_err(|e| ParquetMergeError::InvalidFile(format!("{}: {}", path, e)))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create reader: {}", e)))?
            .with_batch_size(READER_BATCH_SIZE)
            .build()
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to build reader: {}", e)))?;

        let mut file_rows = 0;
        let mut file_batches = 0;
        let file_start_row_id = current_row_id;
        let file_offset = file_offsets[old_file_id.as_str()];

        for batch_result in reader {
            let original_batch = batch_result
                .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to read batch: {}", e)))?;
            let batch_rows = original_batch.num_rows();
            let new_batch = update_row_ids(&original_batch, current_row_id, schema)?;
            writer.write(&new_batch)
                .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to write batch: {}", e)))?;
            current_row_id += batch_rows as i64;
            file_rows += batch_rows;
            file_batches += 1;
        }

        // Build mappings directly into array
        for old_row_id in 0..file_rows as i64 {
            let position = file_offset + old_row_id as usize;
            mapping_array[position] = file_start_row_id + old_row_id;
        }

        stats.files_processed += 1;
        stats.total_rows += file_rows;
        stats.total_batches += file_batches;
    }

    Ok((stats, (mapping_array, file_offsets, file_sizes)))
}

// Row ID update logic
pub fn update_row_ids(
    original_batch: &RecordBatch,
    start_id: i64,
    schema: &SchemaRef,
) -> Result<RecordBatch, Box<dyn Error>> {
    let row_count = original_batch.num_rows();

    // Create new row IDs
    let row_ids: Int64Array = (start_id..start_id + row_count as i64)
        .collect::<Vec<i64>>()
        .into();

    // Build new columns array
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(original_batch.num_columns());

    for (i, column) in original_batch.columns().iter().enumerate() {
        let field_name = schema.field(i).name();
        if field_name == ROW_ID_COLUMN_NAME {
            columns.push(Arc::new(row_ids.clone()));
        } else {
            columns.push(column.clone());
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create batch: {}", e)).into())
}

// JNI helper functions
fn convert_java_list_to_vec(env: &mut JNIEnv, list: JObject) -> Result<Vec<String>, Box<dyn Error>> {
    let iterator = env.call_method(&list, "iterator", "()Ljava/util/Iterator;", &[])?
        .l()?;

    let mut result = Vec::new();
    while env.call_method(&iterator, "hasNext", "()Z", &[])?.z()? {
        let element = env.call_method(&iterator, "next", "()Ljava/lang/Object;", &[])?
            .l()?;
        let path_string = env.call_method(&element, "toString", "()Ljava/lang/String;", &[])?
            .l()?;
        let jstring = JString::from(path_string);
        let string = env.get_string(&jstring)?;
        result.push(string.to_str()?.to_string());
    }

    Ok(result)
}

fn catch_unwind<F: FnOnce() -> Result<(Vec<i64>, std::collections::HashMap<String, usize>, std::collections::HashMap<String, usize>, String), Box<dyn Error>>>(
    f: F
) -> Result<Result<(Vec<i64>, std::collections::HashMap<String, usize>, std::collections::HashMap<String, usize>, String), Box<dyn Error>>, Box<dyn Any + Send>> {
    std::panic::catch_unwind(AssertUnwindSafe(f))
}

// Extract writer generation number from a parquet filename.
// Handles both "_parquet_file_generation_<N>.parquet" and
// "_parquet_file_generation_merged_<N>.parquet".
fn extract_writer_generation(path: &str) -> Option<String> {
    let filename = std::path::Path::new(path)
        .file_stem()  // strip .parquet
        .and_then(|n| n.to_str())?;
    // stem is e.g. "_parquet_file_generation_12" or "_parquet_file_generation_merged_12"
    filename.rsplit('_').next().map(|s| s.to_string())
}

// Create Java RowIdMapping object using compact array + offsets approach
fn create_row_id_mapping_object<'local>(
    env: &mut JNIEnv<'local>,
    mapping_array: Vec<i64>,
    file_offsets: std::collections::HashMap<String, usize>,
    file_sizes: std::collections::HashMap<String, usize>,
    output_file_id: &str,
) -> Result<JObject<'local>, Box<dyn Error>> {
    let total_size = mapping_array.len();

    // Create Java long array
    let j_mapping_array = env.new_long_array(total_size as i32)?;
    env.set_long_array_region(&j_mapping_array, 0, &mapping_array)?;

    // Create Java HashMap for file offsets
    let j_offsets_map = env.new_object("java/util/HashMap", "()V", &[])?;
    for (file_id, offset) in file_offsets {
        let j_file_id = env.new_string(&file_id)?;
        let j_offset = env.new_object("java/lang/Integer", "(I)V", &[JValue::Int(offset as i32)])?;
        env.call_method(&j_offsets_map, "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[JValue::Object(&j_file_id.into()), JValue::Object(&j_offset)])?;
    }

    // Create Java HashMap for file sizes
    let j_sizes_map = env.new_object("java/util/HashMap", "()V", &[])?;
    for (file_id, size) in file_sizes {
        let j_file_id = env.new_string(&file_id)?;
        let j_size = env.new_object("java/lang/Integer", "(I)V", &[JValue::Int(size as i32)])?;
        env.call_method(&j_sizes_map, "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[JValue::Object(&j_file_id.into()), JValue::Object(&j_size)])?;
    }

    // Constructor: ([JLjava/util/Map;Ljava/util/Map;Ljava/lang/String;)V
    let row_id_mapping = env.new_object(
        "org/opensearch/index/engine/exec/merge/RowIdMapping",
        "([JLjava/util/Map;Ljava/util/Map;Ljava/lang/String;)V",
        &[
            JValue::Object(&j_mapping_array.into()),
            JValue::Object(&j_offsets_map),
            JValue::Object(&j_sizes_map),
            JValue::Object(&env.new_string(output_file_id)?.into()),
        ],
    )?;

    Ok(row_id_mapping)
}


// Close function
// #[no_mangle]
// pub extern "system" fn Java_org_opensearch_arrow_bridge_ArrowRustBridge_close(
//     _env: JNIEnv,
//     _class: JClass,
// ) {
//     log_info("Closing ArrowRustBridge");
// }


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_file_id_sorting() {
        let mut file_ids = vec!["0".to_string(), "1".to_string(), "10".to_string(), "2".to_string(), "3".to_string()];

        // Test numeric sorting
        file_ids.sort_by(|a, b| {
            match (a.parse::<i64>(), b.parse::<i64>()) {
                (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
                _ => a.cmp(b),
            }
        });

        assert_eq!(file_ids, vec!["0", "1", "2", "3", "10"]);
        println!("Numeric sort result: {:?}", file_ids);
    }

    #[test]
    fn test_file_offset_mapping() {
        // Simulate the offset calculation logic
        let mut file_sizes = std::collections::HashMap::new();
        file_sizes.insert("0".to_string(), 1);
        file_sizes.insert("1".to_string(), 1);
        file_sizes.insert("10".to_string(), 1);
        file_sizes.insert("2".to_string(), 1);

        let mut sorted_files: Vec<String> = file_sizes.keys().cloned().collect();
        sorted_files.sort_by(|a, b| {
            match (a.parse::<i64>(), b.parse::<i64>()) {
                (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
                _ => a.cmp(b),
            }
        });

        let mut file_offsets = std::collections::HashMap::new();
        let mut current_offset = 0;

        for file_id in &sorted_files {
            file_offsets.insert(file_id.clone(), current_offset);
            current_offset += file_sizes[file_id];
        }

        // Verify offsets
        assert_eq!(file_offsets.get("0"), Some(&0));
        assert_eq!(file_offsets.get("1"), Some(&1));
        assert_eq!(file_offsets.get("2"), Some(&2));
        assert_eq!(file_offsets.get("10"), Some(&3)); // Should be 3 (after 0,1,2)

        println!("File offsets: {:?}", file_offsets);
        println!("Sorted order: {:?}", sorted_files);
    }

    #[test]
    fn test_row_id_mapping_with_multiple_files_multiple_rows() {
        // Simulate 3 files with different row counts
        let mut file_sizes = std::collections::HashMap::new();
        file_sizes.insert("0".to_string(), 5);   // File 0: 5 rows
        file_sizes.insert("1".to_string(), 3);   // File 1: 3 rows
        file_sizes.insert("10".to_string(), 2);  // File 10: 2 rows
        file_sizes.insert("2".to_string(), 4);   // File 2: 4 rows

        // Calculate offsets with numeric sorting
        let mut sorted_files: Vec<String> = file_sizes.keys().cloned().collect();
        sorted_files.sort_by(|a, b| {
            match (a.parse::<i64>(), b.parse::<i64>()) {
                (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
                _ => a.cmp(b),
            }
        });

        println!("Sorted files: {:?}", sorted_files);
        assert_eq!(sorted_files, vec!["0", "1", "2", "10"]);

        let mut file_offsets = std::collections::HashMap::new();
        let mut current_offset = 0;

        for file_id in &sorted_files {
            file_offsets.insert(file_id.clone(), current_offset);
            println!("File '{}': offset={}, size={}", file_id, current_offset, file_sizes[file_id]);
            current_offset += file_sizes[file_id];
        }

        // Verify offsets
        assert_eq!(file_offsets.get("0"), Some(&0));   // Offset 0, rows 0-4
        assert_eq!(file_offsets.get("1"), Some(&5));   // Offset 5, rows 5-7
        assert_eq!(file_offsets.get("2"), Some(&8));   // Offset 8, rows 8-11
        assert_eq!(file_offsets.get("10"), Some(&12)); // Offset 12, rows 12-13

        // Build mapping array
        let total_size = current_offset;
        let mut mapping_array = vec![0i64; total_size];

        let mut new_row_id = 0i64;
        for file_id in &sorted_files {
            let offset = file_offsets[file_id];
            let size = file_sizes[file_id];

            for old_row_id in 0..size as i64 {
                let position = offset + old_row_id as usize;
                mapping_array[position] = new_row_id;
                println!("Mapping: fileId={}, oldRowId={} -> position={}, newRowId={}",
                    file_id, old_row_id, position, new_row_id);
                new_row_id += 1;
            }
        }

        // Verify some mappings
        assert_eq!(mapping_array[0], 0);   // File 0, row 0 -> new row 0
        assert_eq!(mapping_array[4], 4);   // File 0, row 4 -> new row 4
        assert_eq!(mapping_array[5], 5);   // File 1, row 0 -> new row 5
        assert_eq!(mapping_array[7], 7);   // File 1, row 2 -> new row 7
        assert_eq!(mapping_array[8], 8);   // File 2, row 0 -> new row 8
        assert_eq!(mapping_array[12], 12); // File 10, row 0 -> new row 12
        assert_eq!(mapping_array[13], 13); // File 10, row 1 -> new row 13

        println!("Total size: {}", total_size);
        println!("Mapping array: {:?}", mapping_array);
    }
}
