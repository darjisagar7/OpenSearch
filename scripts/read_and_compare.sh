#!/bin/bash
#
# Reads and compares merged parquet and lucene files for a composite index shard.
#
# Usage:
#   ./scripts/read_and_compare.sh [shard_path] [--merged]
#
# Options:
#   --merged    Show only merged Lucene segments (default: show all)
#
# Example:
#   ./scripts/read_and_compare.sh build/testclusters/runTask-0/data/nodes/0/indices/oCM5kuluQiGqGRK_cCWvAA/0
#
# Auto-detect (finds the first index in testclusters):
#   ./scripts/read_and_compare.sh

set -e

# Parse arguments
LUCENE_MODE="--all"
SHARD_PATH=""
for arg in "$@"; do
    case "$arg" in
        --merged) LUCENE_MODE="--merged" ;;
        --all)    LUCENE_MODE="--all" ;;
        *)        SHARD_PATH="$arg" ;;
    esac
done

# Auto-detect shard path if not provided
if [ -z "$SHARD_PATH" ]; then
    SHARD_PATH=$(find build/testclusters -name "parquet" -type d 2>/dev/null | head -1 | sed 's|/parquet$||')
    if [ -z "$SHARD_PATH" ]; then
        echo "ERROR: Could not auto-detect shard path. Provide it as argument."
        exit 1
    fi
    echo "Auto-detected shard path: $SHARD_PATH"
fi

PARQUET_DIR="$SHARD_PATH/parquet"
LUCENE_DIR="$SHARD_PATH/lucene"
INDEX_DIR="$SHARD_PATH/index"
LUCENE_JAR="build/testclusters/runTask-0/distro/3.7.0-ARCHIVE/lib/lucene-core-10.4.0.jar"
SCRIPTS_DIR="$(dirname "$0")"

# ── Step 1: Read Parquet ──
echo ""
echo "======================================================================"
echo "  PARQUET"
echo "======================================================================"

python3 -c "
import pyarrow.parquet as pq
import pandas as pd
import os, sys

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 30)

parquet_dir = '$PARQUET_DIR'
if not os.path.exists(parquet_dir):
    print(f'ERROR: {parquet_dir} not found')
    sys.exit(1)

files = sorted([f for f in os.listdir(parquet_dir) if f.endswith('.parquet')])
merged = [f for f in files if 'merged' in f]
segments = [f for f in files if 'merged' not in f]

print(f'Total segment files: {len(segments)}')
print(f'Merged files: {len(merged)}')

if merged:
    print('\n--- MERGED PARQUET ROWS ---')
    for f in merged:
        path = os.path.join(parquet_dir, f)
        df = pq.read_table(path).to_pandas()
        # Show only meaningful columns
        cols = [c for c in df.columns if not df[c].isna().all()]
        print(f'\nFile: {f} ({len(df)} rows)')
        print(f'Columns with data: {cols}')
        print(df[cols].to_string(index=True))
else:
    print('\nNo merged parquet files found.')
"

# ── Step 2: Read Lucene ──
echo ""
echo "======================================================================"
echo "  LUCENE"
echo "======================================================================"

# Determine which lucene directory to read
# Priority: lucene/ (composite engine dir) > index/ (standard shard dir)
LUCENE_READ_DIR=""
if [ -d "$LUCENE_DIR" ] && [ "$(ls -A "$LUCENE_DIR" 2>/dev/null)" ]; then
    LUCENE_READ_DIR="$LUCENE_DIR"
    echo "Reading from composite lucene dir: $LUCENE_DIR"
elif [ -d "$INDEX_DIR" ] && [ "$(ls -A "$INDEX_DIR" 2>/dev/null)" ]; then
    LUCENE_READ_DIR="$INDEX_DIR"
    echo "Reading from standard index dir: $INDEX_DIR"
else
    echo "No Lucene index files found in either $LUCENE_DIR or $INDEX_DIR"
fi

if [ -n "$LUCENE_READ_DIR" ]; then
    # Compile reader if needed
    if [ ! -f "$SCRIPTS_DIR/ReadLuceneIndex.class" ] || [ "$SCRIPTS_DIR/ReadLuceneIndex.java" -nt "$SCRIPTS_DIR/ReadLuceneIndex.class" ]; then
        echo "Compiling Lucene reader..."
        javac -cp "$LUCENE_JAR" "$SCRIPTS_DIR/ReadLuceneIndex.java" -d "$SCRIPTS_DIR/" 2>/dev/null
    fi

    if [ "$LUCENE_MODE" = "--merged" ]; then
        echo "--- MERGED LUCENE SEGMENTS ONLY ---"
    else
        echo "--- ALL LUCENE SEGMENTS ---"
    fi
    echo ""
    java --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
        -cp "$SCRIPTS_DIR/:$LUCENE_JAR" ReadLuceneIndex "$LUCENE_READ_DIR" "$LUCENE_MODE" 2>/dev/null
fi

# ── Step 3: Compare ──
echo ""
echo "======================================================================"
echo "  COMPARISON"
echo "======================================================================"

python3 -c "
import pyarrow.parquet as pq
import os, sys

parquet_dir = '$PARQUET_DIR'
lucene_dir = '$LUCENE_READ_DIR'

# Read merged parquet
merged_files = sorted([f for f in os.listdir(parquet_dir) if 'merged' in f and f.endswith('.parquet')])
if not merged_files:
    print('No merged parquet files to compare.')
    sys.exit(0)

parquet_rows = []
for f in merged_files:
    t = pq.read_table(os.path.join(parquet_dir, f))
    df = t.to_pandas()
    for _, row in df.iterrows():
        parquet_rows.append(row.to_dict())

print(f'Merged parquet total rows: {len(parquet_rows)}')

# Check row_id sequentiality
row_ids = [r.get('__row_id__') for r in parquet_rows if r.get('__row_id__') is not None]
if row_ids:
    expected = list(range(len(row_ids)))
    if row_ids == expected:
        print(f'✓ __row_id__ is sequential: 0 to {len(row_ids)-1}')
    else:
        print(f'✗ __row_id__ NOT sequential!')
        print(f'  Got:      {row_ids}')
        print(f'  Expected: {expected}')

# Summary table
print(f'\n--- Side-by-side (Parquet merged data) ---')
print(f'{\"row_id\":<8} {\"name\":<15} {\"age\":<8}')
print('-' * 35)
for r in parquet_rows:
    row_id = r.get('__row_id__', '?')
    name = r.get('name', '-')
    age = r.get('age', '-')
    if age != age:  # NaN check
        age = '-'
    print(f'{str(row_id):<8} {str(name):<15} {str(age):<8}')

print()
print('Note: Lucene secondary only indexes text fields (keyword/text) for search.')
print('      - \"name\" is indexed (searchable) but NOT stored as doc values (not retrievable)')
print('      - \"age\" (integer) is NOT indexed in Lucene at all')
print('      - Only __row_id__ is stored as doc values for cross-format correlation')
print('      - Actual field values are retrieved from parquet via __row_id__ lookup')
"
