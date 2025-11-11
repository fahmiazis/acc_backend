import sys
import json
import pandas as pd

import io
# Paksa stdout/stderr pakai UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def main():
    payload = json.load(sys.stdin)
    files = payload.get("files", {})

    main_path = files.get("main")
    mb51_path = files.get("mb51")

    output = {}

    # ====================================================
    # 1. Baca file main.xlsx
    # ====================================================
    xl_main = pd.ExcelFile(main_path)
    for sheet in xl_main.sheet_names:
        df = xl_main.parse(sheet, header=0)  # ambil header row pertama
        # ubah jadi list of lists
        output[sheet] = df.fillna("").values.tolist()

    # ====================================================
    # 2. Baca file mb51.xlsx
    # ====================================================
    if mb51_path:
        df_mb51 = pd.read_excel(mb51_path, header=0)
        output["mb51"] = df_mb51.fillna("").values.tolist()
    else:
        output["mb51"] = []

    # ====================================================
    # 3. Print JSON ke stdout (UTF-8)
    # ====================================================
    json_str = json.dumps(output, ensure_ascii=False)
    print(json_str, flush=True)

if __name__ == "__main__":
    main()
