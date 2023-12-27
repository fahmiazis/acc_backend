const route = require('express').Router()
const divisi = require('../controllers/divisi')

route.post('/create', divisi.createDivisi)
route.get('/get', divisi.getDivisi)
route.patch('/update/:id', divisi.updateDivisi)
route.delete('/delete/:id', divisi.deleteDivisi)
route.get('/detail/:id', divisi.getDetailDivisi)
route.post('/master', divisi.uploadMasterDivisi)
route.get('/export', divisi.exportSqlDivisi)

module.exports = route
