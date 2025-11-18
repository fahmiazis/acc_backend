# generate_inventory_report.py
# Worker Python untuk menghasilkan "Output Report INV ARUS BARANG"
# Semua nilai dihitung di Python dan ditempel sebagai nilai (bukan formula)
# IMPROVED: Added fallback mapping for better BS00/AI00/TR00 data coverage

import sys
import json
import os
import datetime
import traceback
from collections import defaultdict
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font, Border, Side, PatternFill, numbers

def log(msg):
    """Log to stderr so it doesn't interfere with JSON output on stdout"""
    print(f"[worker] {msg}", file=sys.stderr, flush=True)

def find_col(df_cols, candidates):
    """Find first matching column name"""
    lower_map = {}
    for c in df_cols:
        try:
            col_str = str(c).strip()
            if col_str and col_str != 'nan':
                lower_map[col_str.lower()] = c
        except:
            continue
    
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

def safe_to_datetime(series):
    """Convert series to datetime with multiple format attempts"""
    import warnings
    
    formats = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y',
        '%Y/%m/%d', '%d.%m.%Y', '%Y%m%d',
    ]
    
    for fmt in formats:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = pd.to_datetime(series, format=fmt, errors='coerce')
                if result.notna().sum() > len(series) * 0.5:
                    return result
        except:
            continue
    
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pd.to_datetime(series, errors='coerce', dayfirst=True)
    except:
        return pd.to_datetime(series, errors='coerce')

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def get_column_index(letter):
    """Convert Excel column letter to index (1-based)"""
    if len(letter) == 1:
        return ord(letter) - ord('A') + 1
    return sum([(ord(ch) - 64) * (26 ** (len(letter)-i-1)) for i, ch in enumerate(letter)])

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

        log("Loading master data...")
        df_master_inv = pd.DataFrame(master_inventory)
        df_master_mov = pd.DataFrame(master_movement)

        df_master_inv.columns = [str(c).strip().lower() if not pd.isna(c) else f"col_{i}" 
                                  for i, c in enumerate(df_master_inv.columns)]
        df_master_mov.columns = [str(c).strip().lower() if not pd.isna(c) else f"col_{i}" 
                                  for i, c in enumerate(df_master_mov.columns)]
        
        log(f"Master inventory columns: {list(df_master_inv.columns)}")
        log(f"Master movement columns: {list(df_master_mov.columns)}")

        # Create mapping dictionaries
        inv_map = {}
        for _, row in df_master_inv.iterrows():
            plant_key = str(row.get('plant', '')).strip()
            inv_map[plant_key] = {
                'area': str(row.get('area', '')),
                'kode_dist': str(row.get('kode_dist', '')),
                'profit_center': str(row.get('profit_center', '')),
                'channel': str(row.get('channel', '')),
                'status_area': str(row.get('status_area', ''))
            }

        # IMPROVED: Create movement map with 3-key AND 2-key fallback
        mov_map = {}
        mov_map_fallback = {}  # Fallback: mv_type + storage only (ignore mv_text)
        
        for _, row in df_master_mov.iterrows():
            mv_type = str(row.get('mv_type', '')).strip()
            mv_text = str(row.get('mv_text', '')).strip().lower()  # lowercase
            storage_loc = str(row.get('storage_loc', '')).strip().upper()  # UPPERCASE for storage
            
            mv_grouping_val = str(row.get('mv_grouping', '')).strip()
            
            # Primary key: 3-key combination (most specific)
            key = f"{mv_type}|{mv_text}|{storage_loc}"
            mov_map[key] = {
                'mv_grouping': mv_grouping_val,
                'comp_grouping': str(row.get('comp_grouping', '')),
                'saldo': str(row.get('saldo', ''))
            }
            
            # Fallback key: 2-key combination (mv_type + storage only)
            # Only store if not already exists (prioritize more specific match)
            fallback_key = f"{mv_type}|{storage_loc}"
            if fallback_key not in mov_map_fallback and mv_grouping_val:
                mov_map_fallback[fallback_key] = {
                    'mv_grouping': mv_grouping_val,
                    'comp_grouping': str(row.get('comp_grouping', '')),
                    'saldo': str(row.get('saldo', ''))
                }
        
        log(f"Movement map created with {len(mov_map)} entries (3-key combination)")
        log(f"Movement fallback map created with {len(mov_map_fallback)} entries (2-key: mv_type+storage)")
        log(f"Sample movement map keys (first 10):")
        for i, key in enumerate(list(mov_map.keys())[:10]):
            log(f"  '{key}' → '{mov_map[key]['mv_grouping']}'")

        # Read MB51
        log(f"Reading MB51 from: {mb51_path}")
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df_mb51 = pd.read_excel(mb51_path, sheet_name=0, engine="openpyxl", dtype=str)
        
        df_mb51.columns = [str(c).strip() if not pd.isna(c) else f"Unnamed_{i}" 
                          for i, c in enumerate(df_mb51.columns)]
        log(f"MB51 columns found: {list(df_mb51.columns)[:10]}...")
        
        mb_cols = list(df_mb51.columns)
        log(f"Searching for required columns in MB51...")
        
        col_posting = find_col(mb_cols, ["Posting Date"])
        col_material = find_col(mb_cols, ["Material"])
        col_plant = find_col(mb_cols, ["Plant", "Plnt"])
        col_movement = find_col(mb_cols, ["Movement type", "Movement Type"])
        col_movement_text = find_col(mb_cols, ["Movement Type Text"])
        col_amount = find_col(mb_cols, ["Quantity"])
        col_sloc = find_col(mb_cols, ["Storage", "Storage Location", "Storage Loc"])
        col_material_desc = find_col(mb_cols, ["Material description"])

        log(f"  Posting Date: {col_posting}, Material: {col_material}, Plant: {col_plant}")
        log(f"  Movement Type: {col_movement}, Movement Type Text: {col_movement_text}")
        log(f"  Quantity: {col_amount}, Storage: {col_sloc}")

        missing = []
        for name, col in [("Posting Date", col_posting), ("Material", col_material), 
                         ("Plant", col_plant), ("Movement type", col_movement),
                         ("Movement Type Text", col_movement_text),
                         ("Quantity", col_amount)]:
            if not col:
                missing.append(name)
        
        if missing:
            raise ValueError(f"Missing MB51 columns: {', '.join(missing)}")

        # Rename columns
        rename_dict = {
            col_posting: "posting_date",
            col_material: "material",
            col_plant: "plant",
            col_movement: "mv_type",
            col_movement_text: "mv_text",
            col_amount: "amount"
        }
        if col_sloc:
            rename_dict[col_sloc] = "sloc"
        if col_material_desc:
            rename_dict[col_material_desc] = "material_desc_mb51"
        
        df_mb51 = df_mb51.rename(columns=rename_dict)

        # Convert types
        log("Converting data types...")
        
        # Convert posting_date from Excel serial number format
        try:
            df_mb51["posting_date"] = pd.to_datetime(
                df_mb51["posting_date"].astype(float), 
                origin='1899-12-30', 
                unit='D', 
                errors='coerce'
            )
            log(f"  Posting date converted from Excel format")
            log(f"  Sample dates: {df_mb51['posting_date'].head(5).tolist()}")
        except Exception as e:
            log(f"  Warning: Could not convert as Excel date, trying standard formats: {str(e)}")
            df_mb51["posting_date"] = safe_to_datetime(df_mb51["posting_date"])
        
        df_mb51["amount"] = pd.to_numeric(df_mb51["amount"], errors="coerce").fillna(0)
        df_mb51["plant"] = df_mb51["plant"].astype(str).str.strip()
        df_mb51["mv_type"] = df_mb51["mv_type"].astype(str).str.strip()
        df_mb51["material"] = df_mb51["material"].astype(str).str.strip()
        df_mb51["mv_text"] = df_mb51["mv_text"].astype(str).str.strip().str.lower()  # lowercase
        
        # Handle Storage column - empty storage = "TIDAK ADA"
        if 'sloc' in df_mb51.columns:
            df_mb51["storage"] = df_mb51["sloc"].astype(str).str.strip().str.upper()  # UPPERCASE
            df_mb51["storage"] = df_mb51["storage"].replace(['', 'NAN', 'NONE'], 'TIDAK ADA')
        else:
            df_mb51["storage"] = "TIDAK ADA"

        # Map inventory data
        df_mb51["area"] = df_mb51["plant"].map(lambda p: inv_map.get(p, {}).get('area', ''))
        df_mb51["kode_dist"] = df_mb51["plant"].map(lambda p: inv_map.get(p, {}).get('kode_dist', ''))
        df_mb51["profit_center"] = df_mb51["plant"].map(lambda p: inv_map.get(p, {}).get('profit_center', ''))

        # IMPROVED: Map movement data using 3-key combination with fallback to 2-key
        # NORMALIZE to Title Case for consistency
        def get_mv_grouping(row):
            mv_type = str(row['mv_type']).strip()
            mv_text = str(row['mv_text']).strip().lower()
            storage = str(row['storage']).strip().upper()
            
            # Try 3-key first (most specific)
            key = f"{mv_type}|{mv_text}|{storage}"
            result = mov_map.get(key, {}).get('mv_grouping', '')
            
            # If not found, try 2-key fallback (mv_type + storage only)
            if not result:
                fallback_key = f"{mv_type}|{storage}"
                result = mov_map_fallback.get(fallback_key, {}).get('mv_grouping', '')
            
            # Normalize case: Title Case for consistency
            # PEMUSNAHAN -> Pemusnahan, ADJUSTMENT -> Adjustment
            if result:
                result = result.title()
            
            return result
        
        df_mb51["mv_grouping"] = df_mb51.apply(get_mv_grouping, axis=1)
        
        log(f"Sample MB51 data after mapping:")
        if len(df_mb51) > 0:
            sample_cols = ['material', 'plant', 'mv_type', 'mv_text', 'storage', 'mv_grouping', 'amount']
            available_cols = [c for c in sample_cols if c in df_mb51.columns]
            log(f"{df_mb51[available_cols].head(10).to_string()}")
            
            # Show sample lookup keys
            log(f"Sample lookup keys from MB51:")
            for idx in range(min(5, len(df_mb51))):
                row = df_mb51.iloc[idx]
                mv_type = str(row['mv_type']).strip()
                mv_text = str(row['mv_text']).strip().lower()
                storage = str(row['storage']).strip().upper()
                key = f"{mv_type}|{mv_text}|{storage}"
                mv_group = row['mv_grouping']
                log(f"  '{key}' → mv_grouping='{mv_group}'")
        
        log(f"Unique storage values: {list(df_mb51['storage'].unique()[:20])}")
        log(f"Unique mv_grouping: {list(df_mb51['mv_grouping'].unique()[:20])}")
        log(f"Unique mv_type: {list(df_mb51['mv_type'].unique()[:20])}")
        
        # Check how many rows have valid mappings
        valid_storage = df_mb51['storage'].notna() & (df_mb51['storage'] != '') & (df_mb51['storage'] != 'nan')
        valid_mvgroup = df_mb51['mv_grouping'].notna() & (df_mb51['mv_grouping'] != '') & (df_mb51['mv_grouping'] != 'nan')
        log(f"Rows with valid storage: {valid_storage.sum()} / {len(df_mb51)}")
        log(f"Rows with valid mv_grouping: {valid_mvgroup.sum()} / {len(df_mb51)}")
        
        # Show sample of movement mapping
        log(f"Sample movement mapping (first 10 keys):")
        for i, (key, val) in enumerate(list(mov_map.items())[:10]):
            log(f"  {key} → mv_grouping='{val.get('mv_grouping')}')")

        # Determine reporting period from MB51 posting_date
        log("Determining report period from MB51 posting_date...")
        if df_mb51["posting_date"].dropna().empty:
            log("WARNING: No valid posting dates in MB51, using current date")
            report_month_dt = datetime.datetime.now()
        else:
            # Get the max posting date from MB51
            report_month_dt = df_mb51["posting_date"].dropna().max()
            log(f"  Max posting date in MB51: {report_month_dt}")
        
        bulan = report_month_dt.strftime("%B").upper()
        tahun = report_month_dt.year
        bulan_angka = report_month_dt.month
        
        # Calculate previous month
        prev_month_dt = (report_month_dt.replace(day=1) - datetime.timedelta(days=1))
        prev_month = prev_month_dt.strftime("%B").upper()
        prev_year = prev_month_dt.year
        prev_bulan_angka = prev_month_dt.month
        
        bulan_only = bulan  # Use full month name

        log(f"Report period determined:")
        log(f"  Current: {bulan} {tahun} (month {bulan_angka})")
        log(f"  Previous: {prev_month} {prev_year} (month {prev_bulan_angka})")

        # Read main file sheets
        log(f"Reading main file sheets from: {main_path}")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            xls_main = pd.ExcelFile(main_path, engine="openpyxl")
        
        sheets_dict = {}
        for sheet_name in xls_main.sheet_names:
            if sheet_name in ['SALDO AWAL', 'SALDO AWAL MB5B', '13. MB5B', '14. SALDO AKHIR EDS', 
                             'Output Report INV ARUS BARANG', '5. MB51']:
                log(f"  Loading sheet: {sheet_name}")
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    df_sheet = pd.read_excel(main_path, sheet_name=sheet_name, dtype=str)
                    df_sheet.columns = [str(c).strip() if not pd.isna(c) else f"Unnamed_{i}" 
                                       for i, c in enumerate(df_sheet.columns)]
                    sheets_dict[sheet_name] = df_sheet

        # STEP 1: Get existing materials
        existing_materials = []
        if 'Output Report INV ARUS BARANG' in sheets_dict:
            log("Loading existing materials from Output Report sheet...")
            df_existing = sheets_dict['Output Report INV ARUS BARANG']
            log(f"  Sheet shape: {df_existing.shape}")
            
            if df_existing.shape[0] > 8 and df_existing.shape[1] >= 7:
                for idx in range(7, len(df_existing)):  # Start from row 9 (index 8)
                    try:
                        row = df_existing.iloc[idx]
                        plant = str(row.iloc[1]).strip() if len(row) > 1 else ''
                        material = str(row.iloc[5]).strip() if len(row) > 5 else ''
                        
                        if material and material != 'nan' and plant and plant != 'nan':
                            # Try to get from existing data first
                            area = str(row.iloc[0]).strip() if len(row) > 0 and str(row.iloc[0]).strip() != 'nan' else ''
                            kode_dist = str(row.iloc[2]).strip() if len(row) > 2 and str(row.iloc[2]).strip() != 'nan' else ''
                            profit_center = str(row.iloc[3]).strip() if len(row) > 3 and str(row.iloc[3]).strip() != 'nan' else ''
                            
                            # If empty, get from master inventory
                            if not area or area == 'nan':
                                area = inv_map.get(plant, {}).get('area', '')
                            if not kode_dist or kode_dist == 'nan':
                                kode_dist = inv_map.get(plant, {}).get('kode_dist', '')
                            if not profit_center or profit_center == 'nan':
                                profit_center = inv_map.get(plant, {}).get('profit_center', '')
                            
                            existing_materials.append({
                                'material': material,
                                'plant': plant,
                                'area': area,
                                'kode_dist': kode_dist,
                                'profit_center': profit_center,
                                'source': 'existing'
                            })
                    except Exception as e:
                        continue
                
                log(f"  Found {len(existing_materials)} existing materials")
        else:
            log("No existing Output Report sheet found")

        # STEP 2: Get new materials from MB51
        log(f"MB51 total rows: {len(df_mb51)}")
        log(f"MB51 valid dates: {df_mb51['posting_date'].notna().sum()}")
        
        if not df_mb51["posting_date"].dropna().empty:
            df_mb51['year_month'] = df_mb51["posting_date"].dt.to_period('M')
            date_counts = df_mb51['year_month'].value_counts().sort_index()
            log(f"MB51 data by period:")
            for period, count in list(date_counts.items())[:5]:
                log(f"  {period}: {count} rows")
        
        # Filter for current month
        log(f"Filtering MB51 for report period...")
        log(f"  Looking for: year={tahun}, month={bulan_angka}")
        
        df_mb51_filtered = df_mb51[
            (df_mb51["posting_date"].dt.year == tahun) &
            (df_mb51["posting_date"].dt.month == bulan_angka)
        ].copy()

        log(f"Filtered MB51 rows for {bulan} {tahun}: {len(df_mb51_filtered)}")
        
        if len(df_mb51_filtered) == 0:
            log("WARNING: No data for current period, using latest month")
            if not df_mb51["posting_date"].dropna().empty:
                latest_date = df_mb51["posting_date"].dropna().max()
                log(f"  Latest available date: {latest_date}")
                df_mb51_filtered = df_mb51[
                    (df_mb51["posting_date"].dt.year == latest_date.year) &
                    (df_mb51["posting_date"].dt.month == latest_date.month)
                ].copy()
                log(f"  Using fallback: {len(df_mb51_filtered)} rows")
                
                # Update report period to match filtered data
                bulan = latest_date.strftime("%B").upper()
                tahun = latest_date.year
                bulan_angka = latest_date.month
                prev_month_dt = (latest_date.replace(day=1) - datetime.timedelta(days=1))
                prev_month = prev_month_dt.strftime("%B").upper()
                prev_year = prev_month_dt.year
                log(f"  Updated report period: {bulan} {tahun}")
        
        mb51_materials = df_mb51_filtered.groupby(
            ['area', 'plant', 'kode_dist', 'profit_center', 'material'], 
            dropna=False
        ).size().reset_index(name='count')
        
        log(f"Found {len(mb51_materials)} unique materials in MB51")

        # STEP 3: Merge materials
        existing_set = set()
        for em in existing_materials:
            key = f"{em['material']}|{em['plant']}"
            existing_set.add(key)
        
        new_count = 0
        all_materials = existing_materials.copy()
        
        for _, mb_row in mb51_materials.iterrows():
            material = str(mb_row['material']).strip()
            plant = str(mb_row['plant']).strip()
            key = f"{material}|{plant}"
            
            if key not in existing_set and material != 'nan' and plant != 'nan':
                all_materials.append({
                    'material': material,
                    'plant': plant,
                    'area': mb_row['area'],
                    'kode_dist': mb_row['kode_dist'],
                    'profit_center': mb_row['profit_center'],
                    'source': 'new_from_mb51'
                })
                existing_set.add(key)
                new_count += 1
        
        log(f"Added {new_count} new materials")
        log(f"Total materials to process: {len(all_materials)}")
        
        if len(all_materials) == 0:
            raise ValueError("No materials found to process")

        # OPTIMIZE: Pre-aggregate MB51 data with grouping
        log("Pre-aggregating MB51 data for faster processing...")
        
        # Group MB51 by: Material + Plant + Storage + Movement Type + Movement Type Text
        grouped_mb51 = df_mb51_filtered.groupby(
            ['material', 'plant', 'storage', 'mv_type', 'mv_text'],
            dropna=False
        ).agg({
            'amount': 'sum',
            'mv_grouping': 'first'  # Take first value (should be same for all in group)
        }).reset_index()
        
        log(f"  MB51 grouped from {len(df_mb51_filtered)} rows to {len(grouped_mb51)} unique combinations")
        
        log(f"Sample grouped MB51:")
        sample_cols = ['material', 'plant', 'storage', 'mv_type', 'mv_grouping', 'amount']
        log(f"{grouped_mb51[sample_cols].head(10).to_string()}")
        
        # Check mapping success rate
        valid_mvgroup = grouped_mb51['mv_grouping'].notna() & (grouped_mb51['mv_grouping'] != '')
        log(f"  Mapping success: {valid_mvgroup.sum()} / {len(grouped_mb51)} rows have mv_grouping")
        
        # Create fast lookup dictionary: {material|plant|storage|mv_grouping: amount}
        mb51_lookup = {}
        for _, row in grouped_mb51.iterrows():
            material = str(row['material'])
            plant = str(row['plant'])
            storage = str(row['storage'])
            mv_grouping = str(row['mv_grouping'])
            amount = float(row['amount'])
            
            key = f"{material}|{plant}|{storage}|{mv_grouping}"
            mb51_lookup[key] = mb51_lookup.get(key, 0) + amount
        
        log(f"  Created lookup dictionary with {len(mb51_lookup)} unique keys")
        
        # Also create lookup for 641/642 (by mv_type only, no storage)
        mb51_lookup_mv = {}
        for _, row in grouped_mb51.iterrows():
            material = str(row['material'])
            plant = str(row['plant'])
            mv_type = str(row['mv_type'])
            amount = float(row['amount'])
            
            key = f"{material}|{plant}|{mv_type}"
            mb51_lookup_mv[key] = mb51_lookup_mv.get(key, 0) + amount
        
        log(f"  Created mv_type lookup dictionary with {len(mb51_lookup_mv)} unique keys")
        
        # IMPROVED: Debug BS00 data
        bs00_data = grouped_mb51[grouped_mb51['storage'] == 'BS00']
        log(f"  BS00 data found: {len(bs00_data)} rows")
        if len(bs00_data) > 0:
            log(f"  Sample BS00 data:")
            sample_bs00 = bs00_data[['material', 'plant', 'storage', 'mv_type', 'mv_grouping', 'amount']].head(10)
            log(f"{sample_bs00.to_string()}")
            
            # Check unique mv_grouping for BS00
            bs00_mvgroups = bs00_data['mv_grouping'].value_counts()
            log(f"  BS00 mv_grouping distribution:")
            for mvg, count in bs00_mvgroups.items():
                log(f"    '{mvg}': {count} rows")

        grouped_materials = pd.DataFrame(all_materials)

        # Helper Functions - OPTIMIZED with lookup
        def sumifs_mb51(material, mv_grouping_label, storage_loc, plant=None, mv_type_direct=None):
            """Calculate sum from pre-aggregated MB51 lookup
            
            Much faster than filtering DataFrame every time!
            """
            # For 641/642: use mv_type lookup
            if mv_type_direct:
                key = f"{material}|{plant}|{mv_type_direct}"
                return mb51_lookup_mv.get(key, 0.0)
            
            # Normal case: use storage + mv_grouping lookup
            key = f"{material}|{plant}|{storage_loc}|{mv_grouping_label}"
            return mb51_lookup.get(key, 0.0)

        def sumifs_saldo_awal(material, plant, sloc_type):
            if 'SALDO AWAL' not in sheets_dict:
                return 0.0
            df = sheets_dict['SALDO AWAL']
            cols = list(df.columns)
            mat_col = find_col(cols, ["Kode Material", "Material"])
            plant_col = find_col(cols, ["Plant", "Plnt"])
            sloc_col = find_col(cols, ["Storage Loc", "Storage Location"])
            amt_col = find_col(cols, ["Closing Stock (pcs)", "QTY", "Closing Stock"])
            if not mat_col or not amt_col:
                return 0.0
            df_filtered = df[df[mat_col].astype(str).str.strip() == str(material)]
            if plant_col:
                df_filtered = df_filtered[df_filtered[plant_col].astype(str).str.strip() == str(plant)]
            if sloc_col and sloc_type:
                df_filtered = df_filtered[df_filtered[sloc_col].astype(str).str.strip() == str(sloc_type)]
            return float(pd.to_numeric(df_filtered[amt_col], errors='coerce').fillna(0).sum())

        def sumifs_saldo_awal_mb5b(material, plant, sloc_type):
            if 'SALDO AWAL MB5B' not in sheets_dict:
                return 0.0
            df = sheets_dict['SALDO AWAL MB5B']
            if len(df) > 1:
                for i in range(min(5, len(df))):
                    row_values = df.iloc[i].astype(str).str.lower().tolist()
                    if 'material' in ' '.join(row_values):
                        df = df.iloc[i+1:].copy()
                        df.columns = df.iloc[0] if i == 0 else df.columns
                        break
            cols = list(df.columns)
            mat_col = find_col(cols, ["Material"])
            plant_col = find_col(cols, ["Plnt", "Plant"])
            amt_col = find_col(cols, ["GS"]) if sloc_type == "GS" else find_col(cols, ["BS"])
            if not mat_col or not amt_col:
                return 0.0
            df_filtered = df[df[mat_col].astype(str).str.strip() == str(material)]
            if plant_col:
                df_filtered = df_filtered[df_filtered[plant_col].astype(str).str.strip() == str(plant)]
            return float(pd.to_numeric(df_filtered[amt_col], errors='coerce').fillna(0).sum())

        def sumifs_mb5b(material, plant, sloc_type):
            if '13. MB5B' not in sheets_dict:
                return 0.0
            df = sheets_dict['13. MB5B']
            cols = list(df.columns)
            mat_col = find_col(cols, ["Material"])
            plant_col = find_col(cols, ["Plnt", "Plant"])
            amt_col = find_col(cols, ["GS"]) if sloc_type == "GS" else find_col(cols, ["BS"])
            if not mat_col or not amt_col:
                return 0.0
            df_filtered = df[df[mat_col].astype(str).str.strip() == str(material)]
            if plant_col:
                df_filtered = df_filtered[df_filtered[plant_col].astype(str).str.strip() == str(plant)]
            return float(pd.to_numeric(df_filtered[amt_col], errors='coerce').fillna(0).sum())

        def sumifs_eds(material, plant, sloc_type):
            if '14. SALDO AKHIR EDS' not in sheets_dict:
                return 0.0
            df = sheets_dict['14. SALDO AKHIR EDS']
            cols = list(df.columns)
            mat_col = find_col(cols, ["Kode Material", "Material"])
            plant_col = find_col(cols, ["Plant", "Plnt"])
            sloc_col = find_col(cols, ["Storage Loc", "Storage Location"])
            amt_col = find_col(cols, ["Closing Stock (pcs)", "QTY", "Closing Stock"])
            if not mat_col or not amt_col:
                return 0.0
            df_filtered = df[df[mat_col].astype(str).str.strip() == str(material)]
            if plant_col:
                df_filtered = df_filtered[df_filtered[plant_col].astype(str).str.strip() == str(plant)]
            if sloc_col and sloc_type:
                df_filtered = df_filtered[df_filtered[sloc_col].astype(str).str.strip() == str(sloc_type)]
            return float(pd.to_numeric(df_filtered[amt_col], errors='coerce').fillna(0).sum())

        # Load Material Descriptions
        material_desc_map = {}
        if 'material_desc_mb51' in df_mb51.columns:
            for _, r in df_mb51[['material', 'material_desc_mb51']].dropna().iterrows():
                try:
                    mat_key = str(r['material']).strip()
                    mat_desc = str(r['material_desc_mb51']).strip()
                    if mat_key and mat_key != 'nan' and mat_desc and mat_desc != 'nan':
                        material_desc_map[mat_key] = mat_desc
                except:
                    continue
            log(f"Loaded {len(material_desc_map)} material descriptions from MB51")
        
        if 'Output Report INV ARUS BARANG' in sheets_dict:
            try:
                df_out = sheets_dict['Output Report INV ARUS BARANG']
                if df_out.shape[0] > 8 and df_out.shape[1] >= 7:
                    for idx in range(7, len(df_out)):
                        try:
                            row = df_out.iloc[idx]
                            mat_key = str(row.iloc[5]).strip() if len(row) > 5 else ''
                            mat_desc = str(row.iloc[6]).strip() if len(row) > 6 else ''
                            if mat_key and mat_key != 'nan' and mat_desc and mat_desc != 'nan':
                                if mat_key not in material_desc_map:
                                    material_desc_map[mat_key] = mat_desc
                        except:
                            continue
            except Exception as e:
                log(f"Warning: Could not load descriptions from Output Report: {str(e)}")

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

        # HEADER SECTION
        ws["F1"], ws["F2"], ws["F3"], ws["F4"], ws["F5"], ws["F7"] = "Nama Area", "Plant", "Kode Dist", "Profit Center", "Periode", "Material"
        
        if not grouped_materials.empty:
            first_row = grouped_materials.iloc[0]
            ws["G1"] = first_row['area']
            ws["G2"] = first_row['plant']
            ws["G3"] = first_row['kode_dist']
            ws["G4"] = first_row['profit_center']
            ws["G5"] = bulan_only
        
        ws["G7"] = "Material Description"
        ws["A8"], ws["B8"], ws["C8"], ws["D8"], ws["E8"], ws["F8"] = "Nama Area", "Plant", "Kode Dist", "Profit Center", "Periode", "source data"

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
            ("R", "Terima Barang", "DTB"), ("S", "Retur Beli", "BPPR"), ("T", "Penjualan", "LBP"),
            ("U", "Retur Jual", "LBP"), ("V", "Intra Gudang Masuk", "DTB"), ("W", "Intra Gudang", "BPPR"),
            ("X", "Transfer Stock", "ALIH STATUS"), ("Y", "Pemusnahan", "Pemusnahan"), ("Z", "Adjustment", "")
        ]
        for col, label7, label8 in gs00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center
            ws[f"{col}8"] = label8
            ws[f"{col}8"].alignment = center

        ws.merge_cells("AB6:AH6")
        ws["AB6"] = "BS00"
        ws["AB6"].alignment = center
        bs00_movements = [
            ("AB", "Terima Barang", "DTB"), ("AC", "Retur Beli", "BPPR"), ("AD", "Penjualan", "LBP"),
            ("AE", "Retur Jual", "LBP"), ("AF", "Transfer Stock", "ALIH STATUS"), ("AG", "Pemusnahan", "Pemusnahan"), ("AH", "Adjustment", "")
        ]
        for col, label7, label8 in bs00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center
            ws[f"{col}8"] = label8
            ws[f"{col}8"].alignment = center

        ws.merge_cells("AJ6:AQ6")
        ws["AJ6"] = "AI00"
        ws["AJ6"].alignment = center
        ai00_movements = [
            ("AJ", "Terima Barang", "DTB"), ("AK", "Retur Beli", "BPPR"), ("AL", "Penjualan", "LBP"),
            ("AM", "Retur Jual", "LBP"), ("AN", "Intra Gudang", "BPPR"), ("AO", "Transfer Stock", "ALIH STATUS"),
            ("AP", "Pemusnahan", "Pemusnahan"), ("AQ", "Adjustment", "")
        ]
        for col, label7, label8 in ai00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center
            ws[f"{col}8"] = label8
            ws[f"{col}8"].alignment = center

        ws.merge_cells("AS6:AZ6")
        ws["AS6"] = "TR00"
        ws["AS6"].alignment = center
        tr00_movements = [
            ("AS", "Terima Barang", "DTB"), ("AT", "Retur Beli", "BPPR"), ("AU", "Penjualan", "LBP"),
            ("AV", "Retur Jual", "LBP"), ("AW", "Intra Gudang", "BPPR"), ("AX", "Transfer Stock", "ALIH STATUS"),
            ("AY", "Pemusnahan", "Pemusnahan"), ("AZ", "Adjustment", "")
        ]
        for col, label7, label8 in tr00_movements:
            ws[f"{col}7"] = label7
            ws[f"{col}7"].alignment = center
            ws[f"{col}8"] = label8
            ws[f"{col}8"].alignment = center

        ws.merge_cells("BB6:BD6")
        ws["BB6"] = "641 dan 642 tanpa sloc"
        ws["BB6"].alignment = center
        ws["BB7"], ws["BB8"] = "Intra Gudang", "641"
        ws["BC7"], ws["BC8"] = "Intra Gudang", "642"
        ws["BD7"] = "CEK"
        ws["BE3"] = "-->stock in transit"
        ws["BE4"] = "jika selisih cek ke MB5T"

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

        # BODY SECTION
        log("Calculating and writing body rows...")
        write_row = 9
        totals = defaultdict(float)
        
        for idx, mat_row in grouped_materials.iterrows():
            if idx % 50 == 0:
                log(f"  Processing row {idx+1}/{len(grouped_materials)}")
            
            area = str(mat_row['area'])
            plant = str(mat_row['plant'])
            kode_dist = str(mat_row['kode_dist'])
            profit_center = str(mat_row['profit_center'])
            material = str(mat_row['material'])
            material_desc = material_desc_map.get(material, "")

            ws.cell(row=write_row, column=1, value=area)
            ws.cell(row=write_row, column=2, value=plant)
            ws.cell(row=write_row, column=3, value=kode_dist)
            ws.cell(row=write_row, column=4, value=profit_center)
            ws.cell(row=write_row, column=5, value=bulan_only)
            ws.cell(row=write_row, column=6, value=material)
            ws.cell(row=write_row, column=7, value=material_desc)

            h9 = sumifs_saldo_awal(material, plant, "GS")
            i9 = sumifs_saldo_awal(material, plant, "BS")
            j9 = h9 + i9
            ws.cell(row=write_row, column=8, value=h9)
            ws.cell(row=write_row, column=9, value=i9)
            ws.cell(row=write_row, column=10, value=j9)
            
            if idx == 0:
                log(f"  First row: Material={material}, Plant={plant}, H9={h9}, I9={i9}")

            k9 = sumifs_saldo_awal_mb5b(material, plant, "GS")
            l9 = sumifs_saldo_awal_mb5b(material, plant, "BS")
            m9 = k9 + l9
            ws.cell(row=write_row, column=11, value=k9)
            ws.cell(row=write_row, column=12, value=l9)
            ws.cell(row=write_row, column=13, value=m9)

            n9 = h9 - k9
            o9 = i9 - l9
            p9 = n9 + o9
            ws.cell(row=write_row, column=14, value=n9)
            ws.cell(row=write_row, column=15, value=o9)
            ws.cell(row=write_row, column=16, value=p9)

            # GS00 movements
            gs00_values = {}
            for col, label7, _ in gs00_movements:
                val = sumifs_mb51(material, label7, "GS00", plant)
                gs00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val
                
                if idx == 0 and col == "R":
                    log(f"  === DEBUG R9 CALCULATION (LOOKUP METHOD) ===")
                    log(f"  Material: {material}")
                    log(f"  Plant: {plant}")
                    log(f"  mv_grouping_label: '{label7}'")
                    log(f"  storage_loc: 'GS00'")
                    log(f"  Result: {val}")
                    
                    # Debug lookup key
                    lookup_key = f"{material}|{plant}|GS00|{label7}"
                    log(f"  Lookup key: '{lookup_key}'")
                    log(f"  Key exists in mb51_lookup: {lookup_key in mb51_lookup}")
                    
                    # Show similar keys
                    log(f"  Looking for similar keys in mb51_lookup:")
                    similar_keys = [k for k in list(mb51_lookup.keys())[:100] if material in k]
                    if similar_keys:
                        log(f"    Found {len(similar_keys)} keys with material {material}:")
                        for k in similar_keys[:5]:
                            log(f"      '{k}' = {mb51_lookup[k]}")
                    else:
                        log(f"    No keys found with material {material}")
                    
                    # Check grouped_mb51 data
                    test_grouped = grouped_mb51[grouped_mb51['material'] == material]
                    log(f"  Material {material} in grouped_mb51: {len(test_grouped)} rows")
                    if len(test_grouped) > 0:
                        log(f"    Sample grouped data:")
                        display_cols = ['material', 'plant', 'storage', 'mv_type', 'mv_text', 'mv_grouping', 'amount']
                        log(f"{test_grouped[display_cols].head(5).to_string()}")

            # BS00 movements
            bs00_values = {}
            for col, label7, _ in bs00_movements:
                val = sumifs_mb51(material, label7, "BS00", plant)
                bs00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val

            # AI00 movements
            ai00_values = {}
            for col, label7, _ in ai00_movements:
                val = sumifs_mb51(material, label7, "AI00", plant)
                ai00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val

            # TR00 movements
            tr00_values = {}
            for col, label7, _ in tr00_movements:
                val = sumifs_mb51(material, label7, "TR00", plant)
                tr00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val

            # 641/642 - use mv_type_direct parameter
            bb9 = sumifs_mb51(material, None, None, plant, mv_type_direct="641")
            bc9 = sumifs_mb51(material, None, None, plant, mv_type_direct="642")
            bd9 = gs00_values.get("V", 0) - bb9 - bc9
            
            ws.cell(row=write_row, column=get_column_index("BB"), value=bb9)
            ws.cell(row=write_row, column=get_column_index("BC"), value=bc9)
            ws.cell(row=write_row, column=get_column_index("BD"), value=bd9)
            totals["BB"] += bb9
            totals["BC"] += bc9
            totals["BD"] += bd9

            # END STOCK
            sum_gs00 = sum(gs00_values.values())
            sum_ai00 = sum(ai00_values.values())
            sum_tr00 = sum(tr00_values.values())
            bg9 = h9 + sum_gs00 + sum_ai00 + sum_tr00
            bh9 = i9 + sum(bs00_values.values())
            bi9 = bg9 + bh9
            ws.cell(row=write_row, column=get_column_index("BG"), value=bg9)
            ws.cell(row=write_row, column=get_column_index("BH"), value=bh9)
            ws.cell(row=write_row, column=get_column_index("BI"), value=bi9)

            # SAP - MB5B
            bj9 = sumifs_mb5b(material, plant, "GS")
            bk9 = sumifs_mb5b(material, plant, "BS")
            bl9 = bj9 + bk9
            ws.cell(row=write_row, column=get_column_index("BJ"), value=bj9)
            ws.cell(row=write_row, column=get_column_index("BK"), value=bk9)
            ws.cell(row=write_row, column=get_column_index("BL"), value=bl9)

            # DIFF
            bm9 = bg9 - bj9
            bn9 = bh9 - bk9
            bo9 = bm9 + bn9
            ws.cell(row=write_row, column=get_column_index("BM"), value=bm9)
            ws.cell(row=write_row, column=get_column_index("BN"), value=bn9)
            ws.cell(row=write_row, column=get_column_index("BO"), value=bo9)

            bp9 = p9 - bo9
            ws.cell(row=write_row, column=get_column_index("BP"), value=bp9)

            # STOCK - EDS
            br9 = sumifs_eds(material, plant, "GS")
            bs9 = sumifs_eds(material, plant, "BS")
            bt9 = br9 + bs9
            ws.cell(row=write_row, column=get_column_index("BR"), value=br9)
            ws.cell(row=write_row, column=get_column_index("BS"), value=bs9)
            ws.cell(row=write_row, column=get_column_index("BT"), value=bt9)

            bu9 = bj9 - br9
            bv9 = bk9 - bs9
            bw9 = bu9 + bv9
            ws.cell(row=write_row, column=get_column_index("BU"), value=bu9)
            ws.cell(row=write_row, column=get_column_index("BV"), value=bv9)
            ws.cell(row=write_row, column=get_column_index("BW"), value=bw9)

            write_row += 1

        total_rows_written = write_row - 9
        log(f"Total body rows written: {total_rows_written}")
        log(f"  Existing: {len([m for m in all_materials if m['source'] == 'existing'])}")
        log(f"  New: {len([m for m in all_materials if m['source'] == 'new_from_mb51'])}")
        
        # Summary statistics for R-BD columns
        log(f"Summary of R-BD columns (MB51 data):")
        non_zero_cols = []
        for col in ["R", "S", "T", "U", "V", "W", "X", "Y", "Z",
                   "AB", "AC", "AD", "AE", "AF", "AG", "AH",
                   "AJ", "AK", "AL", "AM", "AN", "AO", "AP", "AQ",
                   "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ",
                   "BB", "BC", "BD"]:
            total = totals.get(col, 0)
            if total != 0:
                non_zero_cols.append(f"{col}={total}")
        
        if non_zero_cols:
            log(f"  Columns with data: {', '.join(non_zero_cols[:10])}")
            log(f"  Total columns with data: {len(non_zero_cols)}/35")
        else:
            log(f"  WARNING: All R-BD columns are ZERO!")
            log(f"  This means no MB51 data matched the filters")

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
        
        ws["S1"] = "='5. MB51'!M1-SUM('Output Report INV ARUS BARANG'!R3:BB3)"
        ws["AX2"] = "=X3+AF3+AO3+AX3"
        ws["BL2"] = f"=SUM(BL9:BL{last_row})-SUM('13. MB5B'!P:Q)"

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

        # Apply freeze panes: Freeze rows 1-8 and columns A-G
        ws.freeze_panes = "H9"
        log("Applied freeze panes at H9 (rows 1-8 and columns A-G frozen)")

        # Number format: no decimals
        for row in range(9, write_row):
            for col in range(8, 76):
                cell = ws.cell(row=row, column=col)
                if isinstance(cell.value, (int, float)):
                    cell.number_format = '#,##0'
        
        for row in [2, 3]:
            for col in range(18, 76):
                ws.cell(row=row, column=col).number_format = '#,##0'

        # Save
        log(f"Saving workbook to: {output_path}")
        wb.save(output_path)
        
        if not os.path.exists(output_path):
            raise Exception(f"File was not created at {output_path}")
        
        file_size = os.path.getsize(output_path)
        log(f"File created successfully, size: {file_size} bytes")
        
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
        log("Report generation completed successfully!")

    except Exception as e:
        tb = traceback.format_exc()
        log(f"Error occurred: {str(e)}")
        log(f"Traceback: {tb}")
        
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