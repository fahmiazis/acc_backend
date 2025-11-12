# generate_inventory_report.py - ULTRA-OPTIMIZED VERSION
# Target: Sub-60 seconds
# Key improvements:
# 1. Cached sheet lookups with pre-filtering
# 2. Numpy-based calculations where possible
# 3. Minimal openpyxl calls (batch operations)
# 4. Smart filtering to reduce data size early

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

def safe_to_datetime(series):
    """Convert series to datetime"""
    formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']
    
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

# ULTRA OPTIMIZATION: Pre-build filtered lookup caches
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
                
                # Group and sum
                grouped = df_clean.groupby(['material', 'plant', 'sloc'], dropna=False)['amount'].sum()
                self.caches['saldo_awal'] = grouped.to_dict()
                log(f"  SALDO AWAL cache: {len(self.caches['saldo_awal'])} entries")
        
        # Cache SALDO AWAL MB5B
        if 'SALDO AWAL MB5B' in self.sheets:
            df = self.sheets['SALDO AWAL MB5B']
            # Handle header row
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
        
        # Cache MB5B
        if '13. MB5B' in self.sheets:
            df = self.sheets['13. MB5B']
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
                    self.caches['mb5b_gs'] = grouped_gs.to_dict()
                    log(f"  MB5B GS cache: {len(self.caches['mb5b_gs'])} entries")
                
                if bs_col:
                    df_clean['bs_amount'] = pd.to_numeric(df_clean[bs_col], errors='coerce').fillna(0)
                    grouped_bs = df_clean.groupby(['material', 'plant'], dropna=False)['bs_amount'].sum()
                    self.caches['mb5b_bs'] = grouped_bs.to_dict()
                    log(f"  MB5B BS cache: {len(self.caches['mb5b_bs'])} entries")
        
        # Cache EDS
        if '14. SALDO AKHIR EDS' in self.sheets:
            df = self.sheets['14. SALDO AKHIR EDS']
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
                self.caches['eds'] = grouped.to_dict()
                log(f"  EDS cache: {len(self.caches['eds'])} entries")
    
    def get_saldo_awal(self, material, plant, sloc_type):
        """O(1) lookup"""
        if 'saldo_awal' not in self.caches:
            return 0.0
        return self.caches['saldo_awal'].get((material, plant, sloc_type), 0.0)
    
    def get_mb5b_awal(self, material, plant, sloc_type):
        """O(1) lookup"""
        cache_key = 'mb5b_awal_gs' if sloc_type == "GS" else 'mb5b_awal_bs'
        if cache_key not in self.caches:
            return 0.0
        return self.caches[cache_key].get((material, plant), 0.0)
    
    def get_mb5b(self, material, plant, sloc_type):
        """O(1) lookup"""
        cache_key = 'mb5b_gs' if sloc_type == "GS" else 'mb5b_bs'
        if cache_key not in self.caches:
            return 0.0
        return self.caches[cache_key].get((material, plant), 0.0)
    
    def get_eds(self, material, plant, sloc_type):
        """O(1) lookup"""
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

        # Vectorized mapping creation
        log("Creating lookup dictionaries...")
        df_master_inv['plant'] = df_master_inv['plant'].astype(str).str.strip()
        inv_map = df_master_inv.set_index('plant')[['area', 'kode_dist', 'profit_center']].to_dict('index')
        
        # Movement mapping
        df_master_mov['mv_type'] = df_master_mov['mv_type'].astype(str).str.strip()
        df_master_mov['mv_text'] = df_master_mov['mv_text'].astype(str).str.strip().str.lower()
        df_master_mov['storage_loc'] = df_master_mov['storage_loc'].astype(str).str.strip().str.upper()
        
        df_master_mov['key_3'] = (df_master_mov['mv_type'] + '|' + 
                                   df_master_mov['mv_text'] + '|' + 
                                   df_master_mov['storage_loc'])
        mov_map = df_master_mov.set_index('key_3')['mv_grouping'].to_dict()
        
        df_master_mov['key_2'] = df_master_mov['mv_type'] + '|' + df_master_mov['storage_loc']
        mov_map_fallback = (df_master_mov[df_master_mov['mv_grouping'].notna()]
                           .drop_duplicates('key_2', keep='first')
                           .set_index('key_2')['mv_grouping'].to_dict())
        
        log(f"Movement map: {len(mov_map)} (3-key), {len(mov_map_fallback)} (2-key)")

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

        # Rename and convert
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

        # Convert types
        log("Converting data types...")
        try:
            df_mb51["posting_date"] = pd.to_datetime(
                df_mb51["posting_date"].astype(float), 
                origin='1899-12-30', unit='D', errors='coerce'
            )
        except:
            df_mb51["posting_date"] = safe_to_datetime(df_mb51["posting_date"])
        
        df_mb51["amount"] = pd.to_numeric(df_mb51["amount"], errors="coerce").fillna(0)
        df_mb51["plant"] = df_mb51["plant"].astype(str).str.strip()
        df_mb51["mv_type"] = df_mb51["mv_type"].astype(str).str.strip()
        df_mb51["material"] = df_mb51["material"].astype(str).str.strip()
        df_mb51["mv_text"] = df_mb51["mv_text"].astype(str).str.strip().str.lower()
        
        if 'sloc' in df_mb51.columns:
            df_mb51["storage"] = df_mb51["sloc"].astype(str).str.strip().str.upper()
            df_mb51["storage"] = df_mb51["storage"].replace(['', 'NAN', 'NONE'], 'TIDAK ADA')
        else:
            df_mb51["storage"] = "TIDAK ADA"

        # Vectorized mapping
        log("Mapping data...")
        df_inv_lookup = pd.DataFrame([
            {'plant': k, **v} for k, v in inv_map.items()
        ])
        df_mb51 = df_mb51.merge(
            df_inv_lookup[['plant', 'area', 'kode_dist', 'profit_center']], 
            on='plant', how='left'
        )

        # Movement mapping
        df_mb51['lookup_key_3'] = (df_mb51['mv_type'] + '|' + 
                                    df_mb51['mv_text'] + '|' + 
                                    df_mb51['storage'])
        df_mb51['lookup_key_2'] = df_mb51['mv_type'] + '|' + df_mb51['storage']
        
        df_mb51['mv_grouping'] = df_mb51['lookup_key_3'].map(mov_map)
        df_mb51['mv_grouping'] = df_mb51['mv_grouping'].fillna(
            df_mb51['lookup_key_2'].map(mov_map_fallback)
        )
        df_mb51['mv_grouping'] = df_mb51['mv_grouping'].str.title()
        df_mb51 = df_mb51.drop(columns=['lookup_key_3', 'lookup_key_2'])

        # Determine report period
        if df_mb51["posting_date"].dropna().empty:
            report_month_dt = datetime.datetime.now()
        else:
            report_month_dt = df_mb51["posting_date"].dropna().max()
        
        bulan = report_month_dt.strftime("%B").upper()
        tahun = report_month_dt.year
        bulan_angka = report_month_dt.month
        prev_month_dt = report_month_dt
        prev_month = prev_month_dt.strftime("%B").upper()
        prev_year = prev_month_dt.year
        bulan_only = bulan

        log(f"Report period: {bulan} {tahun}")

        # Parallel sheet reading
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

        # ULTRA OPTIMIZATION: Build sheet cache once
        sheet_cache = SheetCache(sheets_dict)

        # Get existing materials
        existing_materials = []
        if 'Output Report INV ARUS BARANG' in sheets_dict:
            df_existing = sheets_dict['Output Report INV ARUS BARANG']
            if df_existing.shape[0] > 8 and df_existing.shape[1] >= 7:
                df_existing_materials = df_existing.iloc[7:, [0, 1, 2, 3, 5]].copy()
                df_existing_materials.columns = ['area', 'plant', 'kode_dist', 'profit_center', 'material']
                df_existing_materials = df_existing_materials[
                    (df_existing_materials['material'].notna()) & 
                    (df_existing_materials['material'].astype(str).str.strip() != '') &
                    (df_existing_materials['material'].astype(str).str.strip() != 'nan')
                ]
                
                # Fill missing values from master
                for idx, row in df_existing_materials.iterrows():
                    plant = str(row['plant']).strip()
                    if plant in inv_map:
                        if pd.isna(row['area']) or str(row['area']).strip() == '':
                            df_existing_materials.at[idx, 'area'] = inv_map[plant]['area']
                        if pd.isna(row['kode_dist']) or str(row['kode_dist']).strip() == '':
                            df_existing_materials.at[idx, 'kode_dist'] = inv_map[plant]['kode_dist']
                        if pd.isna(row['profit_center']) or str(row['profit_center']).strip() == '':
                            df_existing_materials.at[idx, 'profit_center'] = inv_map[plant]['profit_center']
                
                existing_materials = df_existing_materials.to_dict('records')
                log(f"Found {len(existing_materials)} existing materials")

        # Filter MB51
        df_mb51_filtered = df_mb51[
            (df_mb51["posting_date"].dt.year == tahun) &
            (df_mb51["posting_date"].dt.month == bulan_angka)
        ].copy()

        log(f"Filtered MB51: {len(df_mb51_filtered)} rows")

        # Aggregate MB51
        log("Aggregating MB51...")
        grouped_mb51 = df_mb51_filtered.groupby(
            ['material', 'plant', 'storage', 'mv_type', 'mv_grouping'],
            dropna=False
        ).agg({'amount': 'sum'}).reset_index()
        
        log(f"Grouped MB51: {len(grouped_mb51)} combinations")

        # Create lookups
        grouped_mb51['lookup_key'] = (
            grouped_mb51['material'] + '|' + 
            grouped_mb51['plant'] + '|' + 
            grouped_mb51['storage'] + '|' + 
            grouped_mb51['mv_grouping']
        )
        mb51_lookup = dict(zip(grouped_mb51['lookup_key'], grouped_mb51['amount']))
        
        grouped_mb51['lookup_key_mv'] = (
            grouped_mb51['material'] + '|' + 
            grouped_mb51['plant'] + '|' + 
            grouped_mb51['mv_type']
        )
        mb51_lookup_mv = grouped_mb51.groupby('lookup_key_mv')['amount'].sum().to_dict()

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

        # Helper with lookups
        def sumifs_mb51(material, mv_grouping_label, storage_loc, plant=None, mv_type_direct=None):
            if mv_type_direct:
                key = f"{material}|{plant}|{mv_type_direct}"
                return mb51_lookup_mv.get(key, 0.0)
            key = f"{material}|{plant}|{storage_loc}|{mv_grouping_label}"
            return mb51_lookup.get(key, 0.0)

        # Material descriptions - FIXED: Load from all sources
        log("Loading material descriptions...")
        material_desc_map = {}
        
        # Priority 1: From MB51 (filtered data)
        if 'material_desc_mb51' in df_mb51_filtered.columns:
            desc_df = df_mb51_filtered[['material', 'material_desc_mb51']].dropna()
            desc_df = desc_df[desc_df['material_desc_mb51'].astype(str).str.strip() != '']
            desc_df = desc_df.drop_duplicates('material', keep='first')
            material_desc_map.update(dict(zip(desc_df['material'], desc_df['material_desc_mb51'])))
            log(f"  Loaded {len(material_desc_map)} from MB51 filtered")
        
        # Priority 2: From full MB51 (if filtered didn't have it)
        if 'material_desc_mb51' in df_mb51.columns:
            desc_df = df_mb51[['material', 'material_desc_mb51']].dropna()
            desc_df = desc_df[desc_df['material_desc_mb51'].astype(str).str.strip() != '']
            desc_df = desc_df.drop_duplicates('material', keep='first')
            for mat, desc in zip(desc_df['material'], desc_df['material_desc_mb51']):
                if mat not in material_desc_map:
                    material_desc_map[mat] = desc
            log(f"  Total after full MB51: {len(material_desc_map)}")
        
        # Priority 3: From existing Output Report
        if 'Output Report INV ARUS BARANG' in sheets_dict:
            try:
                df_out = sheets_dict['Output Report INV ARUS BARANG']
                if df_out.shape[0] > 8 and df_out.shape[1] >= 7:
                    for idx in range(7, len(df_out)):
                        try:
                            row = df_out.iloc[idx]
                            mat_key = str(row.iloc[5]).strip() if len(row) > 5 else ''
                            mat_desc = str(row.iloc[6]).strip() if len(row) > 6 else ''
                            if mat_key and mat_key != 'nan' and mat_desc and mat_desc != 'nan' and mat_key not in material_desc_map:
                                material_desc_map[mat_key] = mat_desc
                        except:
                            continue
                    log(f"  Total after Output Report: {len(material_desc_map)}")
            except Exception as e:
                log(f"  Warning loading from Output Report: {str(e)}")
        
        log(f"Final material descriptions loaded: {len(material_desc_map)}")

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

        # ULTRA OPTIMIZATION: Batch calculation with numpy arrays
        log("Calculating body rows (optimized)...")
        write_row = 9
        totals = defaultdict(float)
        
        num_materials = len(grouped_materials)
        
        # Pre-allocate arrays for batch operations
        materials_array = grouped_materials['material'].values
        plants_array = grouped_materials['plant'].values
        
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

            # SALDO AWAL - using cached lookups
            h9 = sheet_cache.get_saldo_awal(material, plant, "GS")
            i9 = sheet_cache.get_saldo_awal(material, plant, "BS")
            j9 = h9 + i9
            ws.cell(row=write_row, column=8, value=h9)
            ws.cell(row=write_row, column=9, value=i9)
            ws.cell(row=write_row, column=10, value=j9)

            k9 = sheet_cache.get_mb5b_awal(material, plant, "GS")
            l9 = sheet_cache.get_mb5b_awal(material, plant, "BS")
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
            for col, label7 in gs00_movements:
                val = sumifs_mb51(material, label7, "GS00", plant)
                gs00_values[col] = val
                ws.cell(row=write_row, column=get_column_index(col), value=val)
                totals[col] += val

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

            # 641/642
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

            # SAP - MB5B (cached)
            bj9 = sheet_cache.get_mb5b(material, plant, "GS")
            bk9 = sheet_cache.get_mb5b(material, plant, "BS")
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

            # STOCK - EDS (cached)
            br9 = sheet_cache.get_eds(material, plant, "GS")
            bs9 = sheet_cache.get_eds(material, plant, "BS")
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

        log(f"Total rows written: {write_row - 9}")

        # Write formulas and calculated values
        log("Writing formulas and calculated values...")
        last_row = write_row - 1
        
        sum_columns = ["R", "S", "T", "U", "V", "W", "X", "Y", "Z",
                      "AB", "AC", "AD", "AE", "AF", "AG", "AH",
                      "AJ", "AK", "AL", "AM", "AN", "AO", "AP", "AQ",
                      "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ",
                      "BB", "BC", "BD"]
        
        for col in sum_columns:
            ws[f"{col}3"] = f"=SUM({col}9:{col}{last_row})"
        
        # S1 - Calculate ctrl balance MB51
        # Original formula: ='5. MB51'!M1-SUM('Output Report INV ARUS BARANG'!R3:BB3)
        # Now: Calculate from MB51 total amount - sum of movements
        log("Calculating S1 (ctrl balance MB51)...")
        
        # Total amount from MB51 (all transactions for the period)
        mb51_total_amount = df_mb51_filtered['amount'].sum()
        
        # Sum of all movement columns (R3:BB3)
        sum_r3_bb3 = sum([totals.get(col, 0) for col in sum_columns])
        
        s1_value = mb51_total_amount - sum_r3_bb3
        ws["S1"] = s1_value
        log(f"  S1 calculated: MB51 total={mb51_total_amount:.2f}, SUM(R3:BB3)={sum_r3_bb3:.2f}, S1={s1_value:.2f}")
        
        # AX2 - Formula between columns (keep as formula)
        ws["AX2"] = "=X3+AF3+AO3+AX3"
        
        # BL2 - Calculate difference with MB5B
        # Original formula: =SUM(BL9:BL{last_row})-SUM('13. MB5B'!P:Q)
        log("Calculating BL2 (MB5B difference)...")
        
        # Sum of BL column (already calculated in the loop)
        sum_bl = 0.0
        for row in range(9, write_row):
            cell_val = ws.cell(row=row, column=get_column_index("BL")).value
            if isinstance(cell_val, (int, float)):
                sum_bl += cell_val
        
        log(f"  BL2 calculation: SUM(BL9:BL{last_row})={sum_bl:.2f}")
        
        # Sum from '13. MB5B' sheet columns P and Q
        sum_mb5b_pq = 0.0
        if '13. MB5B' in sheets_dict:
            df_mb5b_sheet = sheets_dict['13. MB5B']
            try:
                # Column P is index 15 (0-based), Q is index 16
                if df_mb5b_sheet.shape[1] > 16:
                    sum_p = pd.to_numeric(df_mb5b_sheet.iloc[:, 15], errors='coerce').fillna(0).sum()
                    sum_q = pd.to_numeric(df_mb5b_sheet.iloc[:, 16], errors='coerce').fillna(0).sum()
                    sum_mb5b_pq = sum_p + sum_q
                    log(f"  BL2 calculation: SUM('13. MB5B'!P:Q)={sum_mb5b_pq:.2f} (P={sum_p:.2f}, Q={sum_q:.2f})")
                else:
                    log(f"  Warning: '13. MB5B' has only {df_mb5b_sheet.shape[1]} columns, expected at least 17")
            except Exception as e:
                log(f"  Warning: Could not calculate MB5B P:Q sum: {str(e)}")
        else:
            log(f"  Warning: '13. MB5B' sheet not found, using 0 for calculation")
        
        bl2_value = sum_bl - sum_mb5b_pq
        ws["BL2"] = bl2_value
        log(f"  BL2 final value: {sum_bl:.2f} - {sum_mb5b_pq:.2f} = {bl2_value:.2f}")

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

        # Number format - batch
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
        log(f"File created: {file_size} bytes")
        
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
        log("Report completed!")

    except Exception as e:
        tb = traceback.format_exc()
        log(f"Error: {str(e)}")
        log(f"Trace: {tb}")
        
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