#!/usr/bin/env env python3
"""
Download SIMBAD database and prepare denormalized documents for MongoDB insertion.

Each document will contain:
- oid: internal SIMBAD object ID
- main_id: primary identifier
- ra, dec: coordinates
- main_type: primary object classification
- types: list of all object type classifications
- identifiers: list of all alternative identifiers
"""

import argparse
import json
from collections import defaultdict
import os
from pathlib import Path
import pandas as pd
from astroquery.simbad import Simbad
from tqdm import tqdm
import time
from dotenv import load_dotenv

load_dotenv()
# Retrieve output directory from environment variable or use ./ps1_psc/
SIMBAD_OUTPUT_DIR = f"{os.getenv('OUTPUT_DIR','.')}/simbad/"

parser = argparse.ArgumentParser(description="Download all of SIMBAD's data.")
parser.add_argument("--output-dir", type=str, default=SIMBAD_OUTPUT_DIR, help="Directory to save downloaded files")

args = parser.parse_args()
OUTPUT_DIR = args.output_dir
CACHE_DIR = Path(OUTPUT_DIR) / ".cache"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

def get_existing_cache_files(table_name):
    """Get list of existing cache files for a table with their ID ranges."""
    cache_files = list(CACHE_DIR.glob(f"{table_name}_*.parquet"))
    ranges = []
    for f in cache_files:
        # Extract first_id and last_id from filename
        parts = f.stem.split('_')
        if len(parts) >= 3:
            try:
                first_id = int(parts[1])
                last_id = int(parts[2])
                ranges.append((first_id, last_id, f))
            except ValueError:
                continue
    return sorted(ranges, key=lambda x: x[0])

def get_last_cached_id(table_name):
    """Get the last cached ID without loading all data (memory efficient)."""
    cache_files = get_existing_cache_files(table_name)
    if not cache_files:
        return 0
    return cache_files[-1][1]  # Return last_id from last cache file

def load_all_cached_data(table_name):
    """Load all cached data from parquet files (generator for memory efficiency)."""
    cache_files = get_existing_cache_files(table_name)
    for first_id, last_id, filepath in cache_files:
        yield pd.read_parquet(filepath)

def save_cached_batch(df, table_name, first_id, last_id):
    """Save a batch to cache as parquet."""
    cache_file = CACHE_DIR / f"{table_name}_{first_id}_{last_id}.parquet"
    df.to_parquet(cache_file, index=False)

def get_counts():
    """Get total record counts for progress tracking."""
    print("Getting total record counts...\n")
    
    counts = {}
    
    # Count basic objects
    query = """
    SELECT COUNT(*) as total
    FROM basic
    WHERE ra IS NOT NULL AND dec IS NOT NULL
    """
    result = Simbad.query_tap(query)
    counts['basic'] = int(result[0]['total'])
    print(f"  Basic objects: {counts['basic']:,}")
    
    # Count identifiers
    query = """
    SELECT COUNT(*) as total
    FROM ident
    """
    result = Simbad.query_tap(query)
    counts['ident'] = int(result[0]['total'])
    print(f"  Identifiers: {counts['ident']:,}")
    
    # Count types
    query = """
    SELECT COUNT(*) as total
    FROM otypes
    """
    result = Simbad.query_tap(query)
    counts['otypes'] = int(result[0]['total'])
    print(f"  Type classifications: {counts['otypes']:,}")
    
    print()
    return counts

def download_basic_objects(total_count=None):
    """Download core object data (oid, main_id, ra, dec, main type)."""
    print("Step 1/4: Downloading basic object data...")
    
    # Get last cached ID without loading data
    last_oid = get_last_cached_id('basic')
    if last_oid > 0:
        print(f"  Resuming from oid {last_oid}...")
    
    batch_size = 2000000  # SIMBAD's hard limit
    downloaded_count = 0
    
    # Create progress bar
    pbar = tqdm(total=total_count, desc="  Downloading", unit=" objects") if total_count else None
    
    while True:
        query = f"""
        SELECT oid, main_id, ra, dec, otype
        FROM basic
        WHERE ra IS NOT NULL AND dec IS NOT NULL
          AND oid > {last_oid}
        ORDER BY oid
        """
        
        result = Simbad.query_tap(query, maxrec=batch_size)
        
        if result is None or len(result) == 0:
            break
        
        # Convert to DataFrame immediately
        df = result.to_pandas()
        first_oid = int(df.iloc[0]['oid'])
        last_oid = int(df.iloc[-1]['oid'])
        
        # Save to cache
        save_cached_batch(df, 'basic', first_oid, last_oid)
        downloaded_count += len(df)
        
        if pbar:
            pbar.update(len(df))
        
        # If we got fewer records than the batch size, we're done
        if len(df) < batch_size:
            break
    
    if pbar:
        pbar.close()
    
    print(f"  ✓ Downloaded {downloaded_count:,} new objects")
    return downloaded_count > 0

def download_identifiers(total_count=None):
    """Download all alternative identifiers."""
    print("\nStep 2/4: Downloading alternative identifiers...")
    
    # Get last cached ID without loading data
    last_oidref = get_last_cached_id('ident')
    if last_oidref > 0:
        print(f"  Resuming from oidref {last_oidref}...")
    
    batch_size = 2000000  # SIMBAD's hard limit
    downloaded_count = 0
    
    # Create progress bar
    pbar = tqdm(total=total_count, desc="  Downloading", unit=" identifiers") if total_count else None
    
    while True:
        query = f"""
        SELECT oidref, id
        FROM ident
        WHERE oidref > {last_oidref}
        ORDER BY oidref
        """
        
        result = Simbad.query_tap(query, maxrec=batch_size)
        
        if result is None or len(result) == 0:
            break
        
        # Convert to DataFrame immediately
        df = result.to_pandas()
        first_oidref = int(df.iloc[0]['oidref'])
        
        # IMPORTANT: Use second-to-last unique oidref for pagination to avoid
        # losing entries for oidrefs that span multiple batches
        unique_oidrefs = df['oidref'].unique()
        if len(unique_oidrefs) > 1:
            # Use second-to-last unique oidref to ensure last one is complete
            last_oidref = int(unique_oidrefs[-2])
            # Keep only entries up to and including the second-to-last unique oidref
            df = df[df['oidref'] <= last_oidref].copy()
        else:
            # Only one unique oidref in this batch, use it
            last_oidref = int(unique_oidrefs[-1])
        
        # Save to cache (using actual last oidref in the filtered df)
        actual_last_oidref = int(df.iloc[-1]['oidref'])
        save_cached_batch(df, 'ident', first_oidref, actual_last_oidref)
        downloaded_count += len(df)
        
        if pbar:
            pbar.update(len(df))
        
        # If we got fewer records than the batch size, we're done
        if len(result) < batch_size:
            break
    
    if pbar:
        pbar.close()
    
    print(f"  ✓ Downloaded {downloaded_count:,} new identifiers")
    return downloaded_count > 0

def download_all_types(total_count=None):
    """Download all object type classifications."""
    print("\nStep 3/4: Downloading all object classifications...")
    
    # Get last cached ID without loading data
    last_oidref = get_last_cached_id('otypes')
    if last_oidref > 0:
        print(f"  Resuming from oidref {last_oidref}...")
    
    batch_size = 2000000  # SIMBAD's hard limit
    downloaded_count = 0
    
    # Create progress bar
    pbar = tqdm(total=total_count, desc="  Downloading", unit=" classifications") if total_count else None
    
    while True:
        query = f"""
        SELECT oidref, otype
        FROM otypes
        WHERE oidref > {last_oidref}
        ORDER BY oidref
        """
        
        result = Simbad.query_tap(query, maxrec=batch_size)
        
        if result is None or len(result) == 0:
            break
        
        # Convert to DataFrame immediately
        df = result.to_pandas()
        first_oidref = int(df.iloc[0]['oidref'])
        
        # IMPORTANT: Use second-to-last unique oidref for pagination to avoid
        # losing entries for oidrefs that span multiple batches
        unique_oidrefs = df['oidref'].unique()
        if len(unique_oidrefs) > 1:
            # Use second-to-last unique oidref to ensure last one is complete
            last_oidref = int(unique_oidrefs[-2])
            # Keep only entries up to and including the second-to-last unique oidref
            df = df[df['oidref'] <= last_oidref].copy()
        else:
            # Only one unique oidref in this batch, use it
            last_oidref = int(unique_oidrefs[-1])
        
        # Save to cache (using actual last oidref in the filtered df)
        actual_last_oidref = int(df.iloc[-1]['oidref'])
        save_cached_batch(df, 'otypes', first_oidref, actual_last_oidref)
        downloaded_count += len(df)
        
        if pbar:
            pbar.update(len(df))
        
        # If we got fewer records than the batch size, we're done
        if len(result) < batch_size:
            break
    
    if pbar:
        pbar.close()
    
    print(f"  ✓ Downloaded {downloaded_count:,} new type classifications")
    return downloaded_count > 0

def download_type_definitions():
    """Download object type definitions for reference."""
    print("\nStep 4/4: Downloading type definitions...")
    
    query = """
    SELECT otype, label, description
    FROM otypedef
    """
    
    result = Simbad.query_tap(query)
    print(f"  ✓ Downloaded {len(result)} type definitions")
    
    return result

def build_mongodb_documents_streaming(output_file, sample_file='simbad_sample.json', sample_size=100):
    """
    Build denormalized documents and write them incrementally (memory efficient).
    Reads cached parquet files and processes them in batches.
    """
    print("\nBuilding denormalized documents...")
    
    # Step 1: Build lookup dictionaries for identifiers and types
    print("  Building identifier lookup (streaming)...")
    idents_by_oid = defaultdict(list)
    for df_batch in tqdm(load_all_cached_data('ident'), desc="  Processing identifiers"):
        # Remove duplicates within this batch
        df_batch = df_batch.drop_duplicates(subset=['oidref', 'id'], keep='first')
        for _, row in df_batch.iterrows():
            idents_by_oid[int(row['oidref'])].append(str(row['id']))
    
    print("  Building types lookup (streaming)...")
    types_by_oid = defaultdict(list)
    for df_batch in tqdm(load_all_cached_data('otypes'), desc="  Processing types"):
        # Remove duplicates within this batch
        df_batch = df_batch.drop_duplicates(subset=['oidref', 'otype'], keep='first')
        for _, row in df_batch.iterrows():
            oid = int(row['oidref'])
            otype = str(row['otype'])
            if otype not in types_by_oid[oid]:  # Avoid duplicates from overlapping batches
                types_by_oid[oid].append(otype)
    
    # Step 2: Stream basic objects and write documents incrementally
    print(f"  Writing documents to {output_file}...")
    total_docs = 0
    sample_docs = []
    
    with open(output_file, 'w') as f:
        for df_batch in tqdm(load_all_cached_data('basic'), desc="  Creating documents"):
            for _, row in df_batch.iterrows():
                oid = int(row['oid'])
                
                doc = {
                    'oid': oid,
                    'main_id': str(row['main_id']),
                    'ra': float(row['ra']),
                    'dec': float(row['dec']),
                    'main_type': str(row['otype']) if pd.notna(row['otype']) else None,
                    'types': types_by_oid.get(oid, []),
                    'identifiers': idents_by_oid.get(oid, []),
                    'coordinates': {
                        'radec_geojson': {
                            'type': 'Point',
                            'coordinates': [float(row['ra']) - 180.0, float(row['dec'])]
                        }
                    }
                }
                
                f.write(json.dumps(doc) + '\n')
                total_docs += 1
                
                # Save sample
                if len(sample_docs) < sample_size:
                    sample_docs.append(doc)
    
    # Save sample
    if sample_docs:
        with open(sample_file, 'w') as f:
            json.dump(sample_docs, f, indent=2)
        print(f"  ✓ Saved sample of {len(sample_docs)} objects to {sample_file}")
    
    print(f"\n  ✓ Built and saved {total_docs:,} documents to {output_file}")
    return total_docs

def print_statistics(total_docs, sample_file='simbad_sample.json'):
    """Print statistics about the downloaded data (using sample for estimates)."""
    print("\n" + "="*60)
    print("STATISTICS")
    print("="*60)
    
    print(f"Total objects: {total_docs:,}")
    
    # Load sample for statistics
    try:
        with open(sample_file, 'r') as f:
            sample_docs = json.load(f)
        
        sample_size = len(sample_docs)
        print(f"\nSample-based statistics (from {sample_size} objects):")
        
        # Count objects with multiple identifiers
        multi_idents = sum(1 for d in sample_docs if len(d['identifiers']) > 1)
        print(f"Objects with multiple identifiers: ~{100*multi_idents/sample_size:.1f}%")
        
        # Count objects with multiple types
        multi_types = sum(1 for d in sample_docs if len(d['types']) > 1)
        print(f"Objects with multiple types: ~{100*multi_types/sample_size:.1f}%")
        
        # Average identifiers per object
        avg_idents = sum(len(d['identifiers']) for d in sample_docs) / sample_size
        print(f"Average identifiers per object: ~{avg_idents:.2f}")
        
        # Most common types
        type_counts = defaultdict(int)
        for doc in sample_docs:
            if doc['main_type']:
                type_counts[doc['main_type']] += 1
        
        print("\nTop object types in sample:")
        for otype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {otype}: {count}")
    except FileNotFoundError:
        print("  (Sample file not found, skipping detailed statistics)")

def main():
    """Main execution function."""
    print("="*60)
    print("SIMBAD to MongoDB Exporter")
    print("="*60)
    print("\nThis will download the entire SIMBAD database and prepare")
    print("denormalized documents for MongoDB insertion.\n")
    
    start_time = time.time()
    
    try:
        # Get counts first for progress tracking
        counts = get_counts()
        
        # Download data with progress bars (now memory-efficient)
        download_basic_objects(counts['basic'])
        download_identifiers(counts['ident'])
        download_all_types(counts['otypes'])
        type_defs = download_type_definitions()
        
        # Save type definitions separately for reference
        type_def_dict = {
            str(row['otype']): {
                'label': str(row['label']) if row['label'] else None,
                'description': str(row['description']) if row['description'] else None
            }
            for row in type_defs
        }
        with open('simbad_type_definitions.json', 'w') as f:
            json.dump(type_def_dict, f, indent=2)
        print("  ✓ Saved type definitions to simbad_type_definitions.json")
        
        # Build denormalized documents (streaming, memory-efficient)
        total_docs = build_mongodb_documents_streaming(Path(OUTPUT_DIR) / 'simbad_objects.jsonl')
        
        # Print statistics
        print_statistics(total_docs, sample_file=Path(OUTPUT_DIR) / 'simbad_sample.json')
        
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"Total time: {elapsed/60:.1f} minutes")
        print(f"{'='*60}")
        
        print("\n✓ COMPLETE!")
        print("\nTo import into MongoDB, run:")
        print("  mongoimport --db astronomy --collection simbad --file simbad_objects.jsonl")
        
        print("\nYou can also create an index on coordinates for spatial queries:")
        print("  db.simbad.createIndex({ coordinates: '2dsphere' })")
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        raise

if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"\nTotal execution time: {(end - start)/60:.1f} minutes")