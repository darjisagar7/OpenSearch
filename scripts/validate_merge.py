#!/usr/bin/env python3
"""
Validates merged parquet and lucene files for a composite index shard.

Usage:
    python3 scripts/validate_merge.py <shard_data_path>

Example:
    python3 scripts/validate_merge.py build/testclusters/runTask-0/data/nodes/0/indices/oCM5kuluQiGqGRK_cCWvAA/0
"""

import sys
import os
import subprocess
import json

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
except ImportError:
    print("ERROR: pyarrow not installed. Run: pip3 install pyarrow")
    sys.exit(1)


def read_parquet_file(path):
    """Read a parquet file and return as a list of dicts."""
    table = pq.read_table(path)
    return table.to_pandas().to_dict(orient='records'), table


def print_separator(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    shard_path = sys.argv[1]
    parquet_dir = os.path.join(shard_path, "parquet")
    index_dir = os.path.join(shard_path, "index")

    if not os.path.exists(parquet_dir):
        print(f"ERROR: Parquet directory not found: {parquet_dir}")
        sys.exit(1)

    # ── Read all parquet files ──
    print_separator("PARQUET FILES")

    parquet_files = sorted([f for f in os.listdir(parquet_dir) if f.endswith('.parquet')])
    merged_files = [f for f in parquet_files if 'merged' in f]
    segment_files = [f for f in parquet_files if 'merged' not in f]

    print(f"Segment files ({len(segment_files)}):")
    for f in segment_files:
        path = os.path.join(parquet_dir, f)
        table = pq.read_table(path)
        print(f"  {f}: {table.num_rows} rows, {table.num_columns} columns")

    print(f"\nMerged files ({len(merged_files)}):")
    for f in merged_files:
        path = os.path.join(parquet_dir, f)
        table = pq.read_table(path)
        print(f"  {f}: {table.num_rows} rows, {table.num_columns} columns")

    # ── Read and display merged parquet content ──
    if merged_files:
        print_separator("MERGED PARQUET CONTENT")
        for f in merged_files:
            path = os.path.join(parquet_dir, f)
            table = pq.read_table(path)
            df = table.to_pandas()

            print(f"File: {f}")
            print(f"Schema: {table.schema}")
            print(f"Row count: {table.num_rows}")
            print(f"\nData:")
            print(df.to_string(index=True))
            print()

    # ── Read Lucene index using CheckIndex-like approach ──
    print_separator("LUCENE INDEX")

    if os.path.exists(index_dir):
        lucene_files = sorted(os.listdir(index_dir))
        si_files = [f for f in lucene_files if f.endswith('.si')]
        segments_files = [f for f in lucene_files if f.startswith('segments_')]

        print(f"Index directory: {index_dir}")
        print(f"Segment info files: {len(si_files)}")
        for f in si_files:
            size = os.path.getsize(os.path.join(index_dir, f))
            print(f"  {f} ({size} bytes)")

        print(f"\nSegments file: {segments_files}")
        print(f"Total files: {len(lucene_files)}")

        # Show all files grouped by segment
        segments = {}
        for f in lucene_files:
            if f.startswith('segments_') or f == 'write.lock':
                continue
            seg_name = f.split('.')[0]
            segments.setdefault(seg_name, []).append(f)

        print(f"\nSegments breakdown:")
        for seg_name in sorted(segments.keys()):
            files = segments[seg_name]
            total_size = sum(os.path.getsize(os.path.join(index_dir, f)) for f in files)
            print(f"  {seg_name}: {len(files)} files, {total_size} bytes total")
    else:
        print(f"Lucene index directory not found: {index_dir}")

    # ── Validation ──
    print_separator("VALIDATION")

    # Count total rows in all non-merged parquet segment files
    total_segment_rows = 0
    for f in segment_files:
        path = os.path.join(parquet_dir, f)
        table = pq.read_table(path)
        total_segment_rows += table.num_rows

    # Count total rows in merged files
    total_merged_rows = 0
    for f in merged_files:
        path = os.path.join(parquet_dir, f)
        table = pq.read_table(path)
        total_merged_rows += table.num_rows

    print(f"Total rows in segment files: {total_segment_rows}")
    print(f"Total rows in merged files:  {total_merged_rows}")

    # The merged file should contain all the rows from the segments that were merged
    # (not necessarily all segment files, since some may be post-merge)
    if total_merged_rows > 0:
        print(f"\n✓ Merge produced {total_merged_rows} rows")
    else:
        print(f"\n✗ No merged rows found")

    # Validate merged parquet content
    if merged_files:
        print(f"\nMerged file field validation:")
        for f in merged_files:
            path = os.path.join(parquet_dir, f)
            table = pq.read_table(path)
            df = table.to_pandas()

            # Check for expected fields
            columns = list(df.columns)
            print(f"  {f} columns: {columns}")

            # Check for null values
            null_counts = df.isnull().sum()
            if null_counts.any():
                print(f"  Null counts:")
                for col, count in null_counts.items():
                    if count > 0:
                        print(f"    {col}: {count} nulls")

            # Check __row_id__ is present and sequential
            if '__row_id__' in columns:
                row_ids = df['__row_id__'].tolist()
                expected = list(range(len(row_ids)))
                if row_ids == expected:
                    print(f"  ✓ __row_id__ is sequential (0 to {len(row_ids)-1})")
                else:
                    print(f"  ✗ __row_id__ is NOT sequential!")
                    print(f"    Got: {row_ids}")
                    print(f"    Expected: {expected}")
            else:
                print(f"  Note: No __row_id__ column in merged parquet")

            # Check _seq_no is present
            if '_seq_no' in columns:
                seq_nos = sorted(df['_seq_no'].tolist())
                print(f"  _seq_no range: {min(seq_nos)} to {max(seq_nos)}")

            # Check user fields
            if 'name' in columns:
                names = df['name'].dropna().tolist()
                print(f"  'name' values ({len(names)}): {names}")

            if 'age' in columns:
                ages = df['age'].dropna().tolist()
                print(f"  'age' values ({len(ages)}): {ages}")

    # ── Cross-format comparison ──
    # Compare row count: merged parquet rows should match what Lucene merged segment has
    if merged_files and os.path.exists(index_dir):
        print_separator("CROSS-FORMAT COMPARISON")
        print("Note: Full Lucene doc-level comparison requires Java tooling.")
        print("      The Lucene merged segment should have the same doc count as")
        print(f"      the merged parquet file ({total_merged_rows} rows).")
        print()
        print("To verify Lucene doc count, run:")
        print(f"  curl -s 'http://localhost:9200/test-composite-index/_segments' | python3 -m json.tool")


if __name__ == '__main__':
    main()
