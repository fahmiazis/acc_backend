// Material, Plant, Storage, Movement type, Movement Type Text, Special Stock, Document Date, Material Document, Material Doc.Item, Posting Date, Qty in unit of entry, Unit of Entry, Quantity, Base Unit of Measure, Material description, Amt.in loc.cur., Document Header Text, Text, Reference, User Name

// Plnt, Material,           Opening Stock,     Total Receipt Qties,  Total Issue Quantities,           Closing Stock, BUn, GS,  BS , CEK

// Plant, Nama Area, Channel, STATUS, Status Pulau, Storage Loc, Kode Material, Material description, QTY,  Closing Stock (pcs) , Profit Center, Kode Dist

// , 
// Plnt, Material,           Closing Stock, GS, BS

// Plant, Nama Area, Channel, STATUS, Status Pulau, Storage Loc, Kode Material, Material description, QTY,  Closing Stock (pcs) , Profit Center, Kode Dist

const arr = [[2, 3], [4, 5]]
const arrobj = [
    { name: 'test1', plant: 'p100' },
    { name: 'test2', plant: 'p101' },
]

let fullplant = ''
const plant = arrobj.map(x => fullplant = `${fullplant === '' ? '' : fullplant + ', '}${x.plant}`)

console.log(fullplant)
console.log(plant)

// console.log(arr[0][1])

// 300089, P104, BS00, 555, GI scrapping blocked, 25/09/2025, 1231054277, 1, 25/09/2025, -110, PCS, -110, PCS, NABATI RCE 17g GT (10pcs x 12bal) PKU, -71.170, 25P1040000091493, P104/ROM/BS-KSNI/VIII/2025, P104_SA
// 300096, P104, GS00, 601, GD goods issue:delvy, 30/09/2025, 7231578108, 9, 30/09/2025, -1, PAC, -20, PCS, NABATI RCE 5g GT (20pcs x 12ib), -6.326, 4217062603, P104_SA


    
    
    
    
// mv_type, mv_text, mv_grouping, comp_grouping, storage_loc, saldo
// 101, GR goods receipt, Terima Barang, DTB, GS00, +
// 101, GR stock in transit, Intra Gudang Masuk, DTB, GS00, +
// 601, GD delivery sls ord., Penjualan, SALES, GS00
// 601, GD goods issue:delvy, Penjualan, SALES, BS00
// 161, GR returns, Intra Gudang, BPPR, GS00


// Raw stderr: [worker] Loading master data...
// [worker] Master inventory columns: ['id', 'plant', 'area', 'channel', 'profit_center', 'kode_dist', 'pic_inv', 'pic_kasbank', 'status_area', 'createdat', 'updatedat']
// [worker] Master movement columns: ['id', 'mv_type', 'mv_text', 'mv_grouping', 'comp_grouping', 'storage_loc', 'saldo', 'status', 'createdat', 'updatedat']
// [worker] Movement map created with 62 entries (3-key combination)
// [worker] Reading MB51 from: assets/masters/MB51-1761197302836.xlsx
// [worker] MB51 columns found: ['Material', 'Plant', 'Storage', 'Movement type', 'Movement Type Text', 'Special Stock', 'Document Date', 'Material Document', 'Material Doc.Item', 'Posting Date']...
// [worker] Searching for required columns in MB51...
// [worker]   Posting Date: Posting Date, Material: Material, Plant: Plant
// [worker]   Movement Type: Movement type, Movement Type Text: Movement Type Text
// [worker]   Quantity: Quantity, Storage: Storage
// [worker] MB51 Columns: ['Material', 'Plant', 'Storage', 'Movement type', 'Movement Type Text', 'Special Stock', 'Document Date', 'Material Document', 'Material Doc.Item', 'Posting Date', 'Qty in unit of entry', 'Unit of Entry', 'Quantity', 'Base Unit of Measure', 'Material description', 'Amt.in loc.cur.', 'Document Header Text', 'Text', 'Reference', 'User Name']
// [worker] Sample Posting Date after Excel convert: [Timestamp('2025-09-25 00:00:00'), Timestamp('2025-09-30 00:00:00'), Timestamp('2025-09-30 00:00:00'), Timestamp('2025-09-30 00:00:00'), Timestamp('2025-09-30 00:00:00')]
// [worker] Sample MB51 data after mapping:
// [worker]   material plant mv_type               mv_text storage mv_grouping  amount
// 0   300089  P104     555  gi scrapping blocked    BS00  PEMUSNAHAN    -110
// 1   300096  P104     601  gd goods issue:delvy    GS00   Penjualan     -20
// 2   300096  P104     601  gd goods issue:delvy    GS00   Penjualan     -40
// 3   300096  P104     601  gd goods issue:delvy    GS00   Penjualan     -60
// 4   300096  P104     601  gd goods issue:delvy    GS00   Penjualan     -60
// 5   300096  P104     601  gd goods issue:delvy    GS00   Penjualan     -60
// 6   300096  P104     601  gd goods issue:delvy    GS00   Penjualan     -20
// 7   300096  P104     601  gd goods issue:delvy    GS00   Penjualan    -240
// 8   300096  P104     601  gd goods issue:delvy    GS00   Penjualan     -60
// 9   300096  P104     601  gd goods issue:delvy    GS00   Penjualan    -240
// [worker] Unique storage values: ['BS00', 'GS00', 'TIDAK ADA', 'TR00']
// [worker] Unique mv_grouping: ['PEMUSNAHAN', 'Penjualan', 'Intra Gudang Masuk', 'Intra Gudang', '', 'Transfer Stock', 'Terima Barang', 'Retur Jual', 'Retur Beli', 'ADJUSTMENT']
// [worker] Unique mv_type: ['555', '601', '101', '641', '311', '344', '653', '602', '313', '315', '122', 'Z71', 'Y51']
// [worker] Rows with valid storage: 67824 / 67824
// [worker] Rows with valid mv_grouping: 66058 / 67824
// [worker] Sample movement mapping (first 10 keys):
// [worker]   101|gr goods receipt|GS00 \u2192 mv_grouping='Terima Barang')
// [worker]   101|gr stock in transit|GS00 \u2192 mv_grouping='Intra Gudang Masuk')
// [worker]   122|re return to vendor|GS00 \u2192 mv_grouping='Retur Beli')
// [worker]   311|tf trfr within plant|BS00 \u2192 mv_grouping='Transfer Stock')
// [worker]   311|tf trfr within plant|GS00 \u2192 mv_grouping='Transfer Stock')
// [worker]   551|gi scrapping|BS00 \u2192 mv_grouping='Pemusnahan')
// [worker]   641|tf to stck in trans.|TIDAK ADA \u2192 mv_grouping='Intra Gudang')
// [worker]   641|tf to stck in trans.|GS00 \u2192 mv_grouping='Intra Gudang')
// [worker]   Z51|gr retur sales|GS00 \u2192 mv_grouping='Retur Jual')
// [worker]   Z61|gi inventory. pos|GS00 \u2192 mv_grouping='Penjualan')
// [worker] Determining report period from MB51 posting_date...
// [worker]   Max posting date in MB51: 2025-09-30 00:00:00
// [worker] Report period determined:
// [worker]   Current: SEPTEMBER 2025 (month 9)
// [worker]   Previous: AUGUST 2025 (month 8)
// [worker] Reading main file sheets from: assets/masters/main update-1761812477862.xlsx
// [worker]   Loading sheet: Output Report INV ARUS BARANG
// [worker]   Loading sheet: 13. MB5B
// [worker]   Loading sheet: 14. SALDO AKHIR EDS
// [worker]   Loading sheet: SALDO AWAL MB5B
// [worker]   Loading sheet: SALDO AWAL
// [worker] Loading existing materials from Output Report sheet...
// [worker]   Sheet shape: (1627, 75)
// [worker]   Found 1619 existing materials
// [worker] MB51 total rows: 67824
// [worker] MB51 valid dates: 67824
// [worker] MB51 data by period:
// [worker]   2025-09: 67824 rows
// [worker] Filtering MB51 for report period...
// [worker]   Looking for: year=2025, month=9
// [worker] Filtered MB51 rows for SEPTEMBER 2025: 67824
// [worker] Found 131 unique materials in MB51
// [worker] Added 0 new materials
// [worker] Total materials to process: 1619
// [worker] Loaded 131 material descriptions from MB51
// [worker] Creating Excel workbook...
// [worker] Calculating and writing body rows...
// [worker]   Processing row 1/1619
// [worker]   First row: Material=300076, Plant=P104, H9=0.0, I9=0.0
// [worker]   === DEBUG R9 CALCULATION ===
// [worker]   Material: 300076
// [worker]   Plant: P104
// [worker]   mv_grouping_label: 'Terima Barang'
// [worker]   storage_loc: 'GS00'
// [worker]   Result: 0.0
// [worker]   Material 300076 in FULL MB51 (before filter): 0 rows
// [worker]   In FILTERED MB51 (current month only):
// [worker]     After material filter: 0 rows
// [worker]     Material 300076 NOT FOUND in filtered MB51 for current month!
// [worker]     This material might be from existing Output Report (previous month data)
// [worker]     It will show 0 for all MB51 columns (R-BD)