# generate_inventory_report.py - CORRECT LOGIC VERSION
# FIX: Use master_movement as reference to find valid mv_type+mv_text combinations

import sys
import json
import os
import datetime
import traceback
from collections import defaultdict
import pandas as pd
import numpy as np
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from concurrent.futures import ThreadPoolExecutor
import warnings

def log(msg):
    """Log to stderr"""
    print(f"[worker] {msg}", file=sys.stderr, flush=True)

def find_col(df_cols, candidates):
    """Find first matching column name"""
    lower_map = {str(c).strip().lower(): c for c in df_cols if str(c).strip() and str(c).strip() != 'nan'}
    
    for cand in candidates:
        cand_lower = str(cand).lower()
        if cand_lower in lower_map:
            return lower_map[cand_lower]
    
    for cand in candidates:
        cand_lower = str(cand).lower()
        for key, original in lower_map.items():
            if cand_lower in key:
                return original
    return None

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def get_column_index(letter):
    """Convert Excel column letter to index (1-based)"""
    if len(letter) == 1:
        return ord(letter) - ord('A') + 1
    return sum([(ord(ch) - 64) * (26 ** (len(letter)-i-1)) for i, ch in enumerate(letter)])

def read_sheet(file_path, sheet_name):
    """Read single sheet - parallelizable"""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
            df.columns = [str(c).strip() if not pd.isna(c) else f"Unnamed_{i}" 
                         for i, c in enumerate(df.columns)]
            return sheet_name, df
    except Exception as e:
        log(f"Error reading {sheet_name}: {str(e)}")
        return sheet_name, None

class SheetCache:
    """Cache for sheet lookups - build once, query many times"""
    
    def __init__(self, sheets_dict):
        self.sheets = sheets_dict
        self.caches = {}
        self._build_caches()
    
    def _build_caches(self):
        """Pre-build all lookup caches"""
        log("Building sheet lookup caches...")
        
        # Cache SALDO AWAL
        if 'SALDO AWAL' in self.sheets:
            df = self.sheets['SALDO AWAL']
            cols = list(df.columns)
            mat_col = find_col(cols, ["Kode Material", "Material"])
            plant_col = find_col(cols, ["Plant", "Plnt"])
            sloc_col = find_col(cols, ["Storage Loc", "Storage Location"])
            amt_col = find_col(cols, ["Closing Stock (pcs)", "QTY", "Closing Stock"])
            
            if mat_col and amt_col:
                df_clean = df.copy()
                df_clean['material'] = df_clean[mat_col].astype(str).str.strip()
                df_clean['plant'] = df_clean[plant_col].astype(str).str.strip() if plant_col else ''
                df_clean['sloc'] = df_clean[sloc_col].astype(str).str.strip() if sloc_col else ''
                df_clean['amount'] = pd.to_numeric(df_clean[amt_col], errors='coerce').fillna(0)
                
                grouped = df_clean.groupby(['material', 'plant', 'sloc'], dropna=False)['amount'].sum()
                self.caches['saldo_awal'] = grouped.to_dict()
                log(f"  SALDO AWAL cache: {len(self.caches['saldo_awal'])} entries")
        
        # Cache SALDO AWAL MB5B
        if 'SALDO AWAL MB5B' in self.sheets:
            df = self.sheets['SALDO AWAL MB5B']
            
            header_row_idx = None
            if len(df) > 1:
                for i in range(min(10, len(df))):
                    row_values = df.iloc[i].astype(str).str.lower().tolist()
                    if 'material' in ' '.join(row_values):
                        header_row_idx = i
                        break
            
            if header_row_idx is not None:
                df.columns = df.iloc[header_row_idx]
                df = df.iloc[header_row_idx + 1:].reset_index(drop=True)
            
            cols = list(df.columns)
            mat_col = find_col(cols, ["Material"])
            plant_col = find_col(cols, ["Plnt", "Plant"])
            gs_col = find_col(cols, ["GS"])
            bs_col = find_col(cols, ["BS"])
            
            if mat_col and (gs_col or bs_col):
                df_clean = df.copy()
                df_clean['material'] = df_clean[mat_col].astype(str).str.strip()
                df_clean['plant'] = df_clean[plant_col].astype(str).str.strip() if plant_col else ''
                
                if gs_col:
                    df_clean['gs_amount'] = pd.to_numeric(df_clean[gs_col], errors='coerce').fillna(0)
                    grouped_gs = df_clean.groupby(['material', 'plant'], dropna=False)['gs_amount'].sum()
                    self.caches['mb5b_awal_gs'] = grouped_gs.to_dict()
                    log(f"  MB5B AWAL GS cache: {len(self.caches['mb5b_awal_gs'])} entries")
                
                if bs_col:
                    df_clean['bs_amount'] = pd.to_numeric(df_clean[bs_col], errors='coerce').fillna(0)
                    grouped_bs = df_clean.groupby(['material', 'plant'], dropna=False)['bs_amount'].sum()
                    self.caches['mb5b_awal_bs'] = grouped_bs.to_dict()
                    log(f"  MB5B AWAL BS cache: {len(self.caches['mb5b_awal_bs'])} entries")

    def get_saldo_awal(self, material, plant, sloc_type):
        if 'saldo_awal' not in self.caches:
            return 0.0
        return self.caches['saldo_awal'].get((material, plant, sloc_type), 0.0)
    
    def get_mb5b_awal(self, material, plant, sloc_type):
        cache_key = 'mb5b_awal_gs' if sloc_type == "GS" else 'mb5b_awal_bs'
        if cache_key not in self.caches:
            return 0.0
        return self.caches[cache_key].get((material, plant), 0.0)
    
    def get_mb5b(self, material, plant, sloc_type):
        cache_key = 'mb5b_gs' if sloc_type == "GS" else 'mb5b_bs'
        if cache_key not in self.caches:
            return 0.0
        return self.caches[cache_key].get((material, plant), 0.0)
    
    def get_eds(self, material, plant, sloc_type):
        if 'eds' not in self.caches:
            return 0.0
        return self.caches['eds'].get((material, plant, sloc_type), 0.0)

def main():
    try:
        payload = json.load(sys.stdin)
        files = payload.get("files", {})
        master_inventory = payload.get("master_inventory", [])
        master_movement = payload.get("master_movement", [])

        mb51_path = files.get("mb51")
        main_path = files.get("main")

        if not mb51_path or not main_path:
            raise ValueError("Payload must include files.mb51 and files.main paths")

        # Load master data
        log("Loading master data...")
        df_master_inv = pd.DataFrame(master_inventory)
        df_master_mov = pd.DataFrame(master_movement)

        df_master_inv.columns = [str(c).strip().lower() if not pd.isna(c) else f"col_{i}" 
                                  for i, c in enumerate(df_master_inv.columns)]
        df_master_mov.columns = [str(c).strip().lower() if not pd.isna(c) else f"col_{i}" 
                                  for i, c in enumerate(df_master_mov.columns)]

        # Inventory mapping
        log("Creating lookup dictionaries...")
        df_master_inv['plant'] = df_master_inv['plant'].astype(str).str.strip()
        inv_map = df_master_inv.set_index('plant')[['area', 'kode_dist', 'profit_center']].to_dict('index')
        
        # CRITICAL: Build reverse lookup - from (storage_loc, mv_grouping) → list of (mv_type, mv_text)
        log("Building movement reference lookup...")
        df_master_mov['mv_type'] = df_master_mov['mv_type'].astype(str).str.strip()
        df_master_mov['mv_text'] = df_master_mov['mv_text'].astype(str).str.strip().str.lower()
        df_master_mov['storage_loc'] = df_master_mov['storage_loc'].astype(str).str.strip().str.upper()
        df_master_mov['mv_grouping'] = df_master_mov['mv_grouping'].astype(str).str.strip()
        
        # Create lookup: (storage_loc, mv_grouping) → list of valid combinations
        movement_reference = defaultdict(list)
        for _, row in df_master_mov.iterrows():
            if pd.notna(row['mv_grouping']) and row['mv_grouping']:
                key = (row['storage_loc'], row['mv_grouping'])
                movement_reference[key].append({
                    'mv_type': row['mv_type'],
                    'mv_text': row['mv_text']
                })
        
        log(f"  Movement reference: {len(movement_reference)} (storage+grouping) combinations")
        
        # ADDITIONAL: Build lookup by mv_type only (for BB & BC)
        # mv_type → list of UNIQUE mv_text
        movement_by_type = defaultdict(list)
        for _, row in df_master_mov.iterrows():
            if pd.notna(row['mv_type']) and row['mv_type']:
                mv_text = row['mv_text']
                # Avoid duplicates
                if mv_text not in movement_by_type[row['mv_type']]:
                    movement_by_type[row['mv_type']].append(mv_text)
        
        log(f"  Movement by type: {len(movement_by_type)} unique mv_types")
        
        # DEBUG: Show what we have for 641 and 642
        if '641' in movement_by_type:
            log(f"  mv_type '641' has {len(movement_by_type['641'])} unique mv_text:")
            for txt in movement_by_type['641']:
                log(f"    - '{txt}'")
        
        if '642' in movement_by_type:
            log(f"  mv_type '642' has {len(movement_by_type['642'])} unique mv_text:")
            for txt in movement_by_type['642']:
                log(f"    - '{txt}'")
        
        # DEBUG: Show sample
        if len(movement_reference) > 0:
            log("  === Sample Movement Reference (first 5) ===")
            for i, (key, combinations) in enumerate(list(movement_reference.items())[:5]):
                storage, grouping = key
                log(f"    {i+1}. Storage='{storage}', Grouping='{grouping}':")
                for combo in combinations[:3]:  # Show first 3 combos
                    log(f"        → mv_type='{combo['mv_type']}', mv_text='{combo['mv_text']}'")
        
        if len(movement_by_type) > 0:
            log("  === Sample Movement By Type (first 5) ===")
            for i, (mv_type, mv_texts) in enumerate(list(movement_by_type.items())[:5]):
                log(f"    {i+1}. mv_type='{mv_type}': {len(mv_texts)} text variations")
                for txt in mv_texts[:3]:
                    log(f"        → '{txt}'")

        # Read MB51
        log(f"Reading MB51...")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_mb51 = pd.read_excel(mb51_path, sheet_name=0, engine="openpyxl", dtype=str)
        
        df_mb51.columns = [str(c).strip() if not pd.isna(c) else f"Unnamed_{i}" 
                          for i, c in enumerate(df_mb51.columns)]

        # Find columns
        mb_cols = list(df_mb51.columns)
        col_posting = find_col(mb_cols, ["Posting Date"])
        col_material = find_col(mb_cols, ["Material"])
        col_plant = find_col(mb_cols, ["Plant", "Plnt"])
        col_movement = find_col(mb_cols, ["Movement type", "Movement Type"])
        col_movement_text = find_col(mb_cols, ["Movement Type Text"])
        col_amount = find_col(mb_cols, ["Quantity"])
        col_sloc = find_col(mb_cols, ["Storage", "Storage Location", "Storage Loc"])
        col_material_desc = find_col(mb_cols, ["Material description"])

        missing = []
        for name, col in [("Posting Date", col_posting), ("Material", col_material), 
                         ("Plant", col_plant), ("Movement type", col_movement),
                         ("Movement Type Text", col_movement_text), ("Quantity", col_amount)]:
            if not col:
                missing.append(name)
        
        if missing:
            raise ValueError(f"Missing MB51 columns: {', '.join(missing)}")

        # Rename
        rename_dict = {
            col_posting: "posting_date", col_material: "material",
            col_plant: "plant", col_movement: "mv_type",
            col_movement_text: "mv_text", col_amount: "amount"
        }
        if col_sloc:
            rename_dict[col_sloc] = "sloc"
        if col_material_desc:
            rename_dict[col_material_desc] = "material_desc_mb51"
        
        df_mb51 = df_mb51.rename(columns=rename_dict)

        # Convert data types
        log("Converting data types...")
        
        try:
            date_numeric = pd.to_numeric(df_mb51["posting_date"], errors='coerce')
            df_mb51["posting_date"] = pd.to_datetime(
                date_numeric,
                origin='1899-12-30',
                unit='D',
                errors='coerce'
            )
            
            valid_dates = df_mb51["posting_date"].notna().sum()
            log(f"  ✓ Converted {valid_dates}/{len(df_mb51)} dates successfully")
                
        except Exception as e:
            log(f"  ERROR converting dates: {str(e)}")
            df_mb51["posting_date"] = pd.NaT
        
        df_mb51["amount"] = pd.to_numeric(df_mb51["amount"], errors="coerce").fillna(0)
        df_mb51["plant"] = df_mb51["plant"].astype(str).str.strip()
        df_mb51["mv_type"] = df_mb51["mv_type"].astype(str).str.strip()
        df_mb51["material"] = df_mb51["material"].astype(str).str.strip()
        df_mb51["mv_text"] = df_mb51["mv_text"].astype(str).str.strip().str.lower()
        
        # CRITICAL: Handle storage with proper null detection
        if 'sloc' in df_mb51.columns:
            # Mark truly null/empty storage BEFORE any cleaning
            df_mb51["is_empty_storage"] = (
                (df_mb51["sloc"].isna()) |
                (df_mb51["sloc"].isnull()) |
                (df_mb51["sloc"].astype(str).str.strip() == '') |
                (df_mb51["sloc"].astype(str).str.strip().str.upper() == 'NAN') |
                (df_mb51["sloc"].astype(str).str.strip().str.upper() == 'NONE')
            )
            
            # Clean storage for normal lookups
            df_mb51["storage"] = df_mb51["sloc"].astype(str).str.strip().str.upper()
            
            # For empty storage, use special marker
            df_mb51.loc[df_mb51["is_empty_storage"], "storage"] = 'EMPTY_STORAGE'
        else:
            df_mb51["storage"] = "EMPTY_STORAGE"
            df_mb51["is_empty_storage"] = True
        
        # Debug: Show empty storage count
        empty_count = df_mb51["is_empty_storage"].sum()
        log(f"  Rows with empty/null storage: {empty_count}/{len(df_mb51)} ({empty_count/len(df_mb51)*100:.1f}%)")

        # Map inventory
        log("Mapping inventory data...")
        df_inv_lookup = pd.DataFrame([
            {'plant': k, **v} for k, v in inv_map.items()
        ])
        df_mb51 = df_mb51.merge(
            df_inv_lookup[['plant', 'area', 'kode_dist', 'profit_center']], 
            on='plant', how='left'
        )

        # Determine report period
        log("Determining report period...")
        if df_mb51["posting_date"].dropna().empty:
            report_month_dt = datetime.datetime.now()
        else:
            report_month_dt = df_mb51["posting_date"].dropna().max()
        
        bulan = report_month_dt.strftime("%B").upper()
        tahun = report_month_dt.year
        prev_month_dt = report_month_dt
        prev_month = prev_month_dt.strftime("%B").upper()
        prev_year = prev_month_dt.year
        bulan_only = bulan

        log(f"  Report period: {bulan} {tahun}")

        # Read main file sheets
        log("Reading main file sheets...")
        required_sheets = ['SALDO AWAL', 'SALDO AWAL MB5B', '13. MB5B', 
                          '14. SALDO AKHIR EDS', 'Output Report INV ARUS BARANG']
        
        sheets_dict = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(read_sheet, main_path, sheet) for sheet in required_sheets]
            for future in futures:
                sheet_name, df = future.result()
                if df is not None:
                    sheets_dict[sheet_name] = df

        sheet_cache = SheetCache(sheets_dict)

        # Get existing materials
        existing_materials = []
        if 'Output Report INV ARUS BARANG' in sheets_dict:
            df_existing = sheets_dict['Output Report INV ARUS BARANG']
            if df_existing.shape[0] > 8 and df_existing.shape[1] >= 7:
                df_existing_materials = df_existing.iloc[7:, [1, 5]].copy()
                df_existing_materials.columns = ['plant', 'material']
                df_existing_materials = df_existing_materials[
                    (df_existing_materials['material'].notna()) & 
                    (df_existing_materials['material'].astype(str).str.strip() != '') &
                    (df_existing_materials['material'].astype(str).str.strip() != 'nan')
                ]
                
                for idx, row in df_existing_materials.iterrows():
                    plant = str(row['plant']).strip()
                    material = str(row['material']).strip()
                    
                    if plant in inv_map:
                        existing_materials.append({
                            'material': material, 'plant': plant,
                            'area': inv_map[plant]['area'],
                            'kode_dist': inv_map[plant]['kode_dist'],
                            'profit_center': inv_map[plant]['profit_center']
                        })

        # Use ALL MB51 data
        df_mb51_filtered = df_mb51.copy()
        log(f"Using all MB51 data: {len(df_mb51_filtered)} rows")

        # NEW APPROACH: Create lookup by (material, plant, storage, mv_type, mv_text)
        log("Creating MB51 lookup by exact mv_type + mv_text...")
        
        # Debug: Show storage distribution
        log("  === Storage Distribution in MB51 ===")
        storage_counts = df_mb51_filtered['storage'].value_counts().head(10)
        for storage, count in storage_counts.items():
            log(f"    '{storage}': {count} rows")
        
        # Debug: Show empty storage details
        empty_storage_df = df_mb51_filtered[df_mb51_filtered['is_empty_storage'] == True]
        log(f"  === Empty Storage Breakdown ===")
        log(f"  Total empty storage rows: {len(empty_storage_df)}")
        if len(empty_storage_df) > 0:
            log("  Sample empty storage entries (first 3):")
            for idx in range(min(3, len(empty_storage_df))):
                row = empty_storage_df.iloc[idx]
                log(f"    Mat={row['material']}, Plant={row['plant']}, MvType={row['mv_type']}, MvText={row['mv_text'][:30]}, Amt={row['amount']}")
        
        grouped_mb51 = df_mb51_filtered.groupby(
            ['material', 'plant', 'storage', 'mv_type', 'mv_text'],
            dropna=False
        ).agg({'amount': 'sum'}).reset_index()
        
        log(f"  Grouped MB51: {len(grouped_mb51)} combinations")
        
        # Show sample with EMPTY_STORAGE
        empty_storage_sample = grouped_mb51[grouped_mb51['storage'] == 'EMPTY_STORAGE'].head(5)
        if len(empty_storage_sample) > 0:
            log("  === Grouped EMPTY_STORAGE entries (for BB/BC) ===")
            for idx, row in empty_storage_sample.iterrows():
                log(f"    Mat={row['material']}, Plant={row['plant']}, MvType={row['mv_type']}, MvText={row['mv_text'][:30]}, Amt={row['amount']}")
        
        # Create lookup: (material, plant, storage, mv_type, mv_text) → amount
        mb51_lookup = {}
        for _, row in grouped_mb51.iterrows():
            key = (
                row['material'],
                row['plant'],
                row['storage'],
                row['mv_type'],
                row['mv_text']
            )
            mb51_lookup[key] = row['amount']
        
        log(f"  Created mb51_lookup with {len(mb51_lookup)} keys")
        
        # Count EMPTY_STORAGE in lookup
        empty_in_lookup = sum(1 for k in mb51_lookup.keys() if k[2] == 'EMPTY_STORAGE')
        log(f"  Lookup keys with EMPTY_STORAGE: {empty_in_lookup}")
        
        # Show sample
        if len(mb51_lookup) > 0:
            log("  === Sample MB51 Lookup Keys (first 5) ===")
            for i, (key, val) in enumerate(list(mb51_lookup.items())[:5]):
                mat, plt, stor, mvtype, mvtext = key
                log(f"    {i+1}. Mat='{mat}', Plant='{plt}', Storage='{stor}'")
                log(f"        MvType='{mvtype}', MvText='{mvtext[:30]}' => {val}")
        
        # Also create mv_type-only lookup for 641/642
        grouped_mv_type = df_mb51_filtered.groupby(
            ['material', 'plant', 'mv_type'],
            dropna=False
        ).agg({'amount': 'sum'}).reset_index()
        
        mb51_lookup_mv = {}
        for _, row in grouped_mv_type.iterrows():
            key = (row['material'], row['plant'], row['mv_type'])
            mb51_lookup_mv[key] = row['amount']
        
        log(f"  Created mb51_lookup_mv with {len(mb51_lookup_mv)} keys")

        # Merge materials
        mb51_materials = df_mb51_filtered.groupby(
            ['area', 'plant', 'kode_dist', 'profit_center', 'material'], 
            dropna=False
        ).size().reset_index(name='count')
        
        existing_set = set(f"{m['material']}|{m['plant']}" for m in existing_materials)
        new_materials = []
        
        for _, mb_row in mb51_materials.iterrows():
            key = f"{mb_row['material']}|{mb_row['plant']}"
            if key not in existing_set:
                new_materials.append({
                    'material': mb_row['material'], 'plant': mb_row['plant'],
                    'area': mb_row['area'], 'kode_dist': mb_row['kode_dist'],
                    'profit_center': mb_row['profit_center']
                })
        
        all_materials = existing_materials + new_materials
        log(f"Total materials: {len(all_materials)}")

        if len(all_materials) == 0:
            raise ValueError("No materials found")

        grouped_materials = pd.DataFrame(all_materials)

        # CORRECTED SUMIFS function
        lookup_stats = {'found': 0, 'not_found': 0, 'total_calls': 0}
        debug_samples = []
        
        def sumifs_mb51(material, mv_grouping_label, storage_loc, plant, mv_type_direct=None):
            """
            Lookup MB51 data using master_movement as reference
            
            For BB/BC: mv_type_direct is the label from row 8 (e.g., "641", "642")
                       MUST find empty/null storage only
                       Find all mv_text for this mv_type in master_movement
            For normal: use storage_loc + mv_grouping_label
            """
            lookup_stats['total_calls'] += 1
            
            # Special case: BB & BC - lookup by mv_type label with EMPTY storage
            if mv_type_direct:
                # Find all valid mv_text for this mv_type
                valid_texts = movement_by_type.get(mv_type_direct, [])
                
                if not valid_texts:
                    lookup_stats['not_found'] += 1
                    if len(debug_samples) < 10:
                        debug_samples.append(f"NO MV_TEXT for mv_type='{mv_type_direct}'")
                    return 0.0
                
                # CRITICAL: For BB/BC, only look for EMPTY_STORAGE
                total = 0.0
                found_any = False
                details = []  # For debugging first material
                
                for mv_text in valid_texts:
                    lookup_key = (material, plant, 'EMPTY_STORAGE', mv_type_direct, mv_text)
                    amount = mb51_lookup.get(lookup_key, 0.0)
                    if amount != 0:
                        found_any = True
                        details.append(f"mv_text='{mv_text}' => {amount}")
                        if lookup_stats['total_calls'] <= 5:
                            log(f"      BB/BC FOUND (empty storage): {lookup_key} => {amount}")
                    total += amount
                
                # Debug first material BB/BC lookup
                if lookup_stats['total_calls'] <= 2 and len(details) > 0:
                    log(f"    BB/BC Detail for Mat={material}, Plant={plant}, mv_type={mv_type_direct}:")
                    log(f"      Found {len(valid_texts)} mv_text variations from master_movement")
                    for detail in details:
                        log(f"        {detail}")
                    log(f"      TOTAL: {total}")
                
                if found_any:
                    lookup_stats['found'] += 1
                else:
                    lookup_stats['not_found'] += 1
                    if len(debug_samples) < 10:
                        debug_samples.append(f"BB/BC NOT FOUND: Mat='{material}', Plant='{plant}', mv_type='{mv_type_direct}' with EMPTY storage")
                
                return total
            
            # Normal case: lookup using movement reference
            ref_key = (storage_loc, mv_grouping_label)
            valid_combinations = movement_reference.get(ref_key, [])
            
            if not valid_combinations:
                lookup_stats['not_found'] += 1
                if len(debug_samples) < 10:
                    debug_samples.append(f"NO REF: storage='{storage_loc}', grouping='{mv_grouping_label}'")
                return 0.0
            
            # Sum across all valid (mv_type, mv_text) combinations
            total = 0.0
            found_any = False
            
            for combo in valid_combinations:
                lookup_key = (
                    material,
                    plant,
                    storage_loc,
                    combo['mv_type'],
                    combo['mv_text']
                )
                amount = mb51_lookup.get(lookup_key, 0.0)
                if amount != 0:
                    found_any = True
                    if lookup_stats['total_calls'] <= 3:  # Debug first few calls
                        log(f"      FOUND: {lookup_key} => {amount}")
                total += amount
            
            if found_any:
                lookup_stats['found'] += 1
            else:
                lookup_stats['not_found'] += 1
                if len(debug_samples) < 10:
                    debug_samples.append(f"NOT IN MB51: Mat='{material}', Plant='{plant}', Storage='{storage_loc}', Grouping='{mv_grouping_label}'")
            
            return total

        # Material descriptions
        log("Loading material descriptions...")
        material_desc_map = {}
        
        if 'material_desc_mb51' in df_mb51.columns:
            desc_df = df_mb51[['material', 'material_desc_mb51']].dropna()
            desc_df = desc_df[desc_df['material_desc_mb51'].astype(str).str.strip() != '']
            desc_df = desc_df.drop_duplicates('material', keep='first')
            material_desc_map.update(dict(zip(desc_df['material'], desc_df['material_desc_mb51'])))
        
        log(f"  Loaded {len(material_desc_map)} descriptions")

        # Create workbook
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Output_Report_INV_ARUS_BARANG_{timestamp}.xlsx"
        output_dir = os.path.join("assets", "exports")
        ensure_dir(output_dir)
        output_path = os.path.join(output_dir, filename)

        log("Creating Excel workbook...")
        wb = Workbook()
        ws = wb.active
        ws.title = "Output Report INV ARUS BARANG"
        center = Alignment(horizontal="center", vertical="center")

        # HEADER
        ws["F1"], ws["F2"], ws["F3"], ws["F4"], ws["F5"], ws["F7"] = "Nama Area", "Plant", "Kode Dist", "Profit Center", "Periode", "Material"
        
        if not grouped_materials.empty:
            first_row = grouped_materials.iloc[0]
            ws["G1"], ws["G2"], ws["G3"], ws["G4"], ws["G5"] = first_row['area'], first_row['plant'], first_row['kode_dist'], first_row['profit_center'], bulan_only
        
        ws["G7"] = "Material Description"
        ws["A8"], ws["B8"], ws["C8"], ws["D8"], ws["E8"], ws["F8"] = "Nama Area", "Plant", "Kode Dist", "Profit Center", "Periode", "source data"
        
        # Row 8 labels
        ws["R8"] = "DTB"
        ws["S8"] = "BPPR"
        ws["T8"] = "LBP"
        ws["U8"] = "LBP"
        ws["V8"] = "DTB"
        ws["W8"] = "BPPR"
        ws["X8"] = "ALIH STATUS"
        ws["Y8"] = "Pemusnahan"
        
        ws["AB8"] = "DTB"
        ws["AC8"] = "BPPR"
        ws["AD8"] = "LBP"
        ws["AE8"] = "LBP"
        ws["AF8"] = "ALIH STATUS"
        ws["AG8"] = "Pemusnahan"
        
        ws["AJ8"] = "DTB"
        ws["AK8"] = "BPPR"
        ws["AL8"] = "LBP"
        ws["AM8"] = "LBP"
        ws["AN8"] = "BPPR"
        ws["AO8"] = "ALIH STATUS"
        ws["AP8"] = "Pemusnahan"
        
        ws["AS8"] = "DTB"
        ws["AT8"] = "BPPR"
        ws["AU8"] = "LBP"
        ws["AV8"] = "LBP"
        ws["AW8"] = "BPPR"
        ws["AX8"] = "ALIH STATUS"
        ws["AY8"] = "Pemusnahan"
        
        ws["BB8"] = "641"
        ws["BC8"] = "642"

        ws.merge_cells("H4:M4")
        ws["H4"] = f"SALDO AWAL {bulan} {tahun}"
        ws["H4"].alignment = center
        
        ws.merge_cells("H5:J5")
        ws["H5"] = f"SALDO AWAL {prev_month} {prev_year}"
        ws["H5"].alignment = center
        
        ws.merge_cells("K5:M5")
        ws["K5"] = "SAP - MB5B"
        ws["K5"].alignment = center
        ws["N5"] = "DIFF"
        ws["N5"].alignment = center

        headers_6 = ["GS", "BS", "Grand Total", "GS", "BS", "Grand Total", "GS", "BS", "Grand Total"]
        for i, label in enumerate(headers_6, start=8):
            ws.cell(row=6, column=i, value=label).alignment = center

        for col in range(8, 17):
            ws.cell(row=7, column=col, value="S.Aw").alignment = center

        ws["R1"] = "ctrl balance MB51"
        ws.merge_cells("R5:BD5")
        ws["R5"] = "SAP - MB51"
        ws["R5"].alignment = center

        ws.merge_cells("R6:Z6")
        ws["R6"] = "GS00"
        ws["R6"].alignment = center

        gs00_movements = [
            ("R", "Terima Barang"), ("S", "Retur Beli"), ("T", "Penjualan"),
            ("U", "Retur Jual"), ("V", "Intra Gudang Masuk"), ("W", "Intra Gudang"),
            ("X", "Transfer Stock"), ("Y", "Pemusnahan"), ("Z", "Adjustment")
        ]
        for col, label7 in gs00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center

        ws.merge_cells("AB6:AH6")
        ws["AB6"] = "BS00"
        ws["AB6"].alignment = center
        bs00_movements = [
            ("AB", "Terima Barang"), ("AC", "Retur Beli"), ("AD", "Penjualan"),
            ("AE", "Retur Jual"), ("AF", "Transfer Stock"), ("AG", "Pemusnahan"), ("AH", "Adjustment")
        ]
        for col, label7 in bs00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center

        ws.merge_cells("AJ6:AQ6")
        ws["AJ6"] = "AI00"
        ws["AJ6"].alignment = center
        ai00_movements = [
            ("AJ", "Terima Barang"), ("AK", "Retur Beli"), ("AL", "Penjualan"),
            ("AM", "Retur Jual"), ("AN", "Intra Gudang"), ("AO", "Transfer Stock"),
            ("AP", "Pemusnahan"), ("AQ", "Adjustment")
        ]
        for col, label7 in ai00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center

        ws.merge_cells("AS6:AZ6")
        ws["AS6"] = "TR00"
        ws["AS6"].alignment = center
        tr00_movements = [
            ("AS", "Terima Barang"), ("AT", "Retur Beli"), ("AU", "Penjualan"),
            ("AV", "Retur Jual"), ("AW", "Intra Gudang"), ("AX", "Transfer Stock"),
            ("AY", "Pemusnahan"), ("AZ", "Adjustment")
        ]
        for col, label7 in tr00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center

        ws.merge_cells("BB6:BD6")
        ws["BB6"] = "641 dan 642 tanpa sloc"
        ws["BB6"].alignment = center
        ws["BB7"], ws["BC7"], ws["BD7"] = "Intra Gudang", "Intra Gudang", "CEK"
        ws["BE3"], ws["BE4"] = "-->stock in transit", "jika selisih cek ke MB5T"

        ws.merge_cells("BG4:BL4")
        ws["BG4"] = f"END STOCK {prev_month} {prev_year}"
        ws["BG4"].alignment = center
        ws.merge_cells("BG5:BI5")
        ws["BG5"] = "SALDO AKHIR"
        ws["BG5"].alignment = center
        ws.merge_cells("BJ5:BL5")
        ws["BJ5"] = "SAP - MB5B"
        ws["BJ5"].alignment = center
        ws["BM5"] = "DIFF"
        ws["BM5"].alignment = center

        ws["BG6"], ws["BH6"], ws["BI6"] = "GS00", "BS00", "Grand Total"
        ws["BJ6"], ws["BK6"], ws["BL6"] = "GS", "BS", "Grand Total"
        ws["BM6"], ws["BN6"], ws["BO6"] = "GS", "BS", "Grand Total"

        for col in range(59, 68):
            ws.cell(row=6, column=col).alignment = center
            ws.cell(row=7, column=col, value="S.Ak").alignment = center

        ws["BP7"] = "CEK SELISIH VS BULAN LALU"
        ws["BQ7"] = "kalo ada selisih atas inputan LOG1, LOG2 -> konfirmasi pa Reza utk diselesaikan"

        ws.merge_cells("BR5:BT5")
        ws["BR5"] = "STOCK - EDS"
        ws["BR5"].alignment = center
        ws["BU5"] = "DIFF"
        ws["BU5"].alignment = center

        ws["BR6"], ws["BS6"], ws["BT6"] = "GS", "BS", "Grand Total"
        ws["BU6"], ws["BV6"], ws["BW6"] = "GS", "BS", "Grand Total"

        for col in range(70, 76):
            ws.cell(row=6, column=col).alignment = center
            ws.cell(row=7, column=col, value="S.Ak").alignment = center

        # BODY CALCULATION
        log("Calculating body rows...")
        write_row = 9
        totals = defaultdict(float)
        
        num_materials = len(grouped_materials)
        
        for idx in range(num_materials):
            if idx % 100 == 0:
                log(f"  Processing {idx}/{num_materials}")
            
            mat_row = grouped_materials.iloc[idx]
            area = str(mat_row['area'])
            plant = str(mat_row['plant'])
            kode_dist = str(mat_row['kode_dist'])
            profit_center = str(mat_row['profit_center'])
            material = str(mat_row['material'])
            material_desc = material_desc_map.get(material, "")

            # Write basic info
            ws.cell(row=write_row, column=1, value=area)
            ws.cell(row=write_row, column=2, value=plant)
            ws.cell(row=write_row, column=3, value=kode_dist)
            ws.cell(row=write_row, column=4, value=profit_center)
            ws.cell(row=write_row, column=5, value=bulan_only)
            ws.cell(row=write_row, column=6, value=material)
            ws.cell(row=write_row, column=7, value=material_desc)
            
            # Debug first material
            if idx == 0:
                log(f"  === FIRST MATERIAL DEBUG ===")
                log(f"  Material: {material}, Plant: {plant}")

            # SALDO AWAL - Mix of values and formulas
            h9 = sheet_cache.get_saldo_awal(material, plant, "GS")
            i9 = sheet_cache.get_saldo_awal(material, plant, "BS")
            j9_formula = f"=H{write_row}+I{write_row}"
            
            ws.cell(row=write_row, column=8, value=h9)
            ws.cell(row=write_row, column=9, value=i9)
            ws.cell(row=write_row, column=10, value=j9_formula)

            k9 = sheet_cache.get_mb5b_awal(material, plant, "GS")
            l9 = sheet_cache.get_mb5b_awal(material, plant, "BS")
            m9_formula = f"=SUM(K{write_row}:L{write_row})"
            
            ws.cell(row=write_row, column=11, value=k9)
            ws.cell(row=write_row, column=12, value=l9)
            ws.cell(row=write_row, column=13, value=m9_formula)

            n9_formula = f"=H{write_row}-K{write_row}"
            o9_formula = f"=I{write_row}-L{write_row}"
            p9_formula = f"=N{write_row}+O{write_row}"
            
            ws.cell(row=write_row, column=14, value=n9_formula)
            ws.cell(row=write_row, column=15, value=o9_formula)
            ws.cell(row=write_row, column=16, value=p9_formula)

            # GS00 movements
            gs00_values = {}
            for col, label7 in gs00_movements:
                val = sumifs_mb51(material, label7, "GS00", plant)
                gs00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val
                
                if idx == 0:
                    log(f"    GS00 {col} ({label7}): {val}")

            # BS00 movements
            bs00_values = {}
            for col, label7 in bs00_movements:
                val = sumifs_mb51(material, label7, "BS00", plant)
                bs00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val

            # AI00 movements
            ai00_values = {}
            for col, label7 in ai00_movements:
                val = sumifs_mb51(material, label7, "AI00", plant)
                ai00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val

            # TR00 movements
            tr00_values = {}
            for col, label7 in tr00_movements:
                val = sumifs_mb51(material, label7, "TR00", plant)
                tr00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val

            # 641/642 - Use mv_type from row 8 labels
            bb9 = sumifs_mb51(material, None, None, plant, mv_type_direct="641")
            bc9 = sumifs_mb51(material, None, None, plant, mv_type_direct="642")
            
            # BD9 - FORMULA ONLY (not calculated)
            bd9_formula = f"=V{write_row}-BB{write_row}-BC{write_row}"
            
            ws.cell(row=write_row, column=get_column_index("BB"), value=bb9)
            ws.cell(row=write_row, column=get_column_index("BC"), value=bc9)
            ws.cell(row=write_row, column=get_column_index("BD"), value=bd9_formula)
            totals["BB"] += bb9
            totals["BC"] += bc9
            # BD is formula, don't add to totals

            # END STOCK - Some are formulas
            sum_gs00 = sum(gs00_values.values())
            sum_ai00 = sum(ai00_values.values())
            sum_tr00 = sum(tr00_values.values())
            
            # BG9 = H9 + SUM(R9:Z9) + SUM(AJ9:AZ9) - FORMULA
            bg9_formula = f"=H{write_row}+SUM(R{write_row}:Z{write_row})+SUM(AJ{write_row}:AZ{write_row})"
            
            # BH9 = I9 + SUM(AB9:AH9) - FORMULA
            bh9_formula = f"=I{write_row}+SUM(AB{write_row}:AH{write_row})"
            
            # BI9 = BG9 + BH9 - FORMULA
            bi9_formula = f"=BG{write_row}+BH{write_row}"
            
            ws.cell(row=write_row, column=get_column_index("BG"), value=bg9_formula)
            ws.cell(row=write_row, column=get_column_index("BH"), value=bh9_formula)
            ws.cell(row=write_row, column=get_column_index("BI"), value=bi9_formula)

            # SAP - MB5B - Mix of values and formulas
            bj9 = sheet_cache.get_mb5b(material, plant, "GS")
            bk9 = sheet_cache.get_mb5b(material, plant, "BS")
            bl9_formula = f"=SUM(BJ{write_row}:BK{write_row})"
            
            ws.cell(row=write_row, column=get_column_index("BJ"), value=bj9)
            ws.cell(row=write_row, column=get_column_index("BK"), value=bk9)
            ws.cell(row=write_row, column=get_column_index("BL"), value=bl9_formula)

            # DIFF - All formulas
            bm9_formula = f"=BG{write_row}-BJ{write_row}"
            bn9_formula = f"=BH{write_row}-BK{write_row}"
            bo9_formula = f"=BM{write_row}+BN{write_row}"
            
            ws.cell(row=write_row, column=get_column_index("BM"), value=bm9_formula)
            ws.cell(row=write_row, column=get_column_index("BN"), value=bn9_formula)
            ws.cell(row=write_row, column=get_column_index("BO"), value=bo9_formula)

            bp9_formula = f"=P{write_row}-BO{write_row}"
            ws.cell(row=write_row, column=get_column_index("BP"), value=bp9_formula)

            # STOCK - EDS - Mix of values and formulas
            br9 = sheet_cache.get_eds(material, plant, "GS")
            bs9 = sheet_cache.get_eds(material, plant, "BS")
            bt9_formula = f"=BR{write_row}+BS{write_row}"
            
            ws.cell(row=write_row, column=get_column_index("BR"), value=br9)
            ws.cell(row=write_row, column=get_column_index("BS"), value=bs9)
            ws.cell(row=write_row, column=get_column_index("BT"), value=bt9_formula)

            bu9_formula = f"=BJ{write_row}-BR{write_row}"
            bv9_formula = f"=BK{write_row}-BS{write_row}"
            bw9_formula = f"=BU{write_row}+BV{write_row}"
            
            ws.cell(row=write_row, column=get_column_index("BU"), value=bu9_formula)
            ws.cell(row=write_row, column=get_column_index("BV"), value=bv9_formula)
            ws.cell(row=write_row, column=get_column_index("BW"), value=bw9_formula)

            write_row += 1

        log(f"Total rows written: {write_row - 9}")
        
        # Show lookup stats
        log(f"  === LOOKUP STATISTICS ===")
        log(f"  Total lookup calls: {lookup_stats['total_calls']}")
        log(f"  Successful: {lookup_stats['found']}")
        log(f"  Not found: {lookup_stats['not_found']}")
        
        if len(debug_samples) > 0:
            log(f"  === DEBUG SAMPLES (first 10 issues) ===")
            for sample in debug_samples[:10]:
                log(f"    {sample}")
        
        # Check totals
        has_values = sum(1 for v in totals.values() if v != 0)
        log(f"  Columns with non-zero totals: {has_values}/{len(totals)}")
        if has_values > 0:
            log(f"  === NON-ZERO TOTALS (sample) ===")
            for i, (col, val) in enumerate(sorted(totals.items())):
                if i >= 10:
                    break
                if val != 0:
                    log(f"    {col}: {val:,.2f}")

        # Write formulas
        log("Writing formulas...")
        last_row = write_row - 1
        
        sum_columns = ["R", "S", "T", "U", "V", "W", "X", "Y", "Z",
                      "AB", "AC", "AD", "AE", "AF", "AG", "AH",
                      "AJ", "AK", "AL", "AM", "AN", "AO", "AP", "AQ",
                      "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ",
                      "BB", "BC", "BD"]
        
        for col in sum_columns:
            ws[f"{col}3"] = f"=SUM({col}9:{col}{last_row})"
        
        # S1
        mb51_total_amount = df_mb51_filtered['amount'].sum()
        sum_r3_bb3 = sum([totals.get(col, 0) for col in sum_columns])
        s1_value = mb51_total_amount - sum_r3_bb3
        ws["S1"] = s1_value
        log(f"  S1 = {s1_value:.2f}")
        
        # AX2
        ws["AX2"] = "=X3+AF3+AO3+AX3"
        
        # BL2
        sum_bl = 0.0
        for row in range(9, write_row):
            cell_val = ws.cell(row=row, column=get_column_index("BL")).value
            if isinstance(cell_val, (int, float)):
                sum_bl += cell_val
        
        sum_mb5b_pq = 0.0
        if '13. MB5B' in sheets_dict:
            df_mb5b_sheet = sheets_dict['13. MB5B']
            try:
                if df_mb5b_sheet.shape[1] > 16:
                    sum_p = pd.to_numeric(df_mb5b_sheet.iloc[:, 15], errors='coerce').fillna(0).sum()
                    sum_q = pd.to_numeric(df_mb5b_sheet.iloc[:, 16], errors='coerce').fillna(0).sum()
                    sum_mb5b_pq = sum_p + sum_q
            except Exception as e:
                log(f"  Warning: {str(e)}")
        
        bl2_value = sum_bl - sum_mb5b_pq
        ws["BL2"] = bl2_value
        log(f"  BL2 = {bl2_value:.2f}")

        # Formatting
        log("Formatting...")
        for i in range(1, 76):
            ws.column_dimensions[get_column_letter(i)].width = 12
        
        ws.column_dimensions['Q'].width = 2
        ws.column_dimensions['AA'].width = 2
        ws.column_dimensions['AI'].width = 2
        ws.column_dimensions['AR'].width = 2
        ws.column_dimensions['BA'].width = 2
        ws.column_dimensions['BF'].width = 4

        ws.freeze_panes = "H9"

        # Number format
        for row in range(9, write_row):
            for col in range(8, 76):
                cell = ws.cell(row=row, column=col)
                if isinstance(cell.value, (int, float)):
                    cell.number_format = '#,##0'
        
        for row in [2, 3]:
            for col in range(18, 76):
                ws.cell(row=row, column=col).number_format = '#,##0'

        # Save
        log(f"Saving workbook...")
        wb.save(output_path)
        
        if not os.path.exists(output_path):
            raise Exception(f"File was not created")
        
        file_size = os.path.getsize(output_path)
        log(f"✓ File created: {file_size:,} bytes")
        
        result = {
            "success": True,
            "output_path": output_path,
            "rows_written": write_row - 9,
            "report_month": f"{bulan} {tahun}",
            "total_materials": len(grouped_materials),
            "file_size": file_size,
            "timestamp": timestamp
        }
        
        print(json.dumps(result))
        sys.stdout.flush()
        log("✓ Report completed successfully!")

    except Exception as e:
        tb = traceback.format_exc()
        log(f"ERROR: {str(e)}")
        log(f"Traceback:\n{tb}")
        
        error_result = {
            "success": False,
            "error": str(e),
            "trace": tb
        }
        
        print(json.dumps(error_result))
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()