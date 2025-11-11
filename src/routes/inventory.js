const route = require('express').Router()
const inventory = require('../controllers/inventory')

route.post('/add', inventory.addInventory)
route.get('/all', inventory.getInventory)
route.get('/get', inventory.getAllInventory)
route.get('/detail/:id', inventory.getDetailInventory)
route.patch('/update/:id', inventory.updateInventory)
route.post('/master', inventory.uploadMasterInventory)
route.patch('/delete', inventory.deleteInventory)
route.delete('/delall', inventory.deleteAll)
route.get('/export', inventory.exportSqlInventory)

// report inventory
route.post('/report/upload', inventory.uploadReportInv)
route.post('/report/update', inventory.updateReportInv)
route.patch('/report/delete', inventory.deleteReport)
route.get('/report/get', inventory.getAllReport)
route.get('/report/detail/:id', inventory.getDetailReport)
route.patch('/report/generate', inventory.generateInventoryReport)
route.patch('/report/merge', inventory.mergeInventoryReports)

module.exports = route
