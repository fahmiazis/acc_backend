const route = require('express').Router()
const dokumen = require('../controllers/documents')

route.post('/add', dokumen.addDocument)
route.get('/area/get', dokumen.getDocumentsArea)
route.get('/get', dokumen.getDocuments)
route.patch('/update/:id', dokumen.updateDocument)
route.patch('/delete', dokumen.deleteDocuments)
route.get('/detail/:id', dokumen.getDetailDocument)
// route.patch('/upload/:id', dokumen.uploadDocument)
route.patch('/reject/:id', dokumen.rejectDocument)
route.patch('/approve/:id', dokumen.approveDocument)
route.post('/master', dokumen.uploadMasterDokumen)
route.get('/export', dokumen.exportSqlDocument)

module.exports = route
