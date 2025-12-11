# filter_mb51_worker.py - Simple MB51 Filter Worker (Total Quantity Only)
import sys
import json
import pandas as pd
import warnings
import traceback

def log(msg):
    print(f"[filter] {msg}", file=sys.stderr, flush=True)

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

def main():
    try:
        # Terima payload dari JS
        payload = json.load(sys.stdin)
        files = payload.get("files", {})
        mb51_path = files.get("mb51")
        
        if not mb51_path:
            raise ValueError("MB51 file path required in payload.files.mb51")
        
        # Baca MB51
        log(f"Reading MB51: {mb51_path}")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_excel(mb51_path, sheet_name=0, dtype=str)
        
        df.columns = [str(c).strip() if not pd.isna(c) else f"Unnamed_{i}" 
                     for i, c in enumerate(df.columns)]
        
        log(f"Total rows: {len(df)}")
        
        # Cari kolom yang diperlukan
        cols = list(df.columns)
        col_storage = find_col(cols, ["Storage", "Storage Loc", "SLoc", "Stor. Location"])
        col_movement = find_col(cols, ["Movement type", "Movement Type", "Mvmt. Type"])
        col_movement_text = find_col(cols, ["Movement Type Text", "Movement type text", "Mvt"])
        col_quantity = find_col(cols, ["Quantity", "Qty", "Quantity in UnE"])
        col_material = find_col(cols, ["Material", "Materi", "Mtr"])
        
        if not col_storage:
            raise ValueError(f"Storage column not found")
        if not col_movement:
            raise ValueError(f"Movement Type column not found")
        if not col_movement_text:
            raise ValueError(f"Movement Type Text column not found")
        if not col_quantity:
            raise ValueError(f"Quantity column not found")
        if not col_material:
            raise ValueError(f"Material column not found")
        
        log(f"Columns mapped:")
        log(f"  Storage: '{col_storage}'")
        log(f"  Movement Type: '{col_movement}'")
        log(f"  Movement Type Text: '{col_movement_text}'")
        log(f"  Quantity: '{col_quantity}'")
        
        # Clean data
        df['storage_clean'] = df[col_storage].astype(str).str.strip().str.upper()
        df['movement_clean'] = df[col_movement].astype(str).str.strip()
        df['text_clean'] = df[col_movement_text].astype(str).str.strip()
        df['material_clean'] = df[col_material].astype(str).str.strip()

        df['quantity_clean'] = pd.to_numeric(df[col_quantity], errors='coerce').fillna(0)
        
        # Apply filter logic
        log("Applying filter logic:")
        log("  ✓ Storage = 'GS00'")
        log("  ✓ Movement Type IN ('101', '123', '501', '502')")
        log("  ✓ Movement Type Text IN ('GR goods receipt', 'RE rtrn vendor rev.', 'Receipt w/o PO', 'RE receipt w/o PO')")
        
        # Filter
        # mask_storage = df['storage_clean'] == 'GS00'
        # mask_storage = df['storage_clean'].isin(['GS00'])
        mask_storage = (
            (df['storage_clean'] == '') | 
            (df['storage_clean'] == 'NAN') | 
            (df['storage_clean'] == 'NONE') |
            (df[col_storage].isna()) |
            (df[col_storage].isnull())
        )
        # mask_movement = df['movement_clean'].isin(['101', '123', '501', '502'])
        mask_movement = df['movement_clean'].isin(['641'])
        # mask_text = df['text_clean'].isin(['GR goods receipt', 'RE rtrn vendor rev.', 'Receipt w/o PO', 'RE receipt w/o PO'])
        mask_text = df['text_clean'].isin(['TF to stck in trans.'])
        mask_material = df['material_clean'].isin(['303168'])
        
        filtered = df[mask_storage & mask_movement & mask_text].copy()
        
        log(f"Filtered: {len(filtered)}/{len(df)} rows ({len(filtered)/len(df)*100:.1f}%)")
        
        # Calculate total quantity
        total_quantity = filtered['quantity_clean'].sum()
        
        log(f"Total Quantity: {total_quantity:,.2f}")
        
        # Return hasil
        result = {
            "success": True,
            "total_rows": len(df),
            "filtered_rows": len(filtered),
            "total_quantity": float(total_quantity)
        }
        
        print(json.dumps(result))
        sys.stdout.flush()
        log("✓ Filter completed successfully!")
        
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