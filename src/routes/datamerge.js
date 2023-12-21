const route = require('express').Router()
const datamerge = require('../controllers/datamerge')

route.get('/all', datamerge.getDataMerge)
route.get('/get', datamerge.getAllDataMerge)
route.get('/detail/:no', datamerge.getDetailDataMerge)
route.post('/excel', datamerge.uploadMasterDataMerge)
route.post('/rar', datamerge.uploadRar)
route.delete('/del/:id', datamerge.deleteDataMerge)
route.delete('/delall', datamerge.deleteAll)
route.get('/export', datamerge.exportSqlDataMerge)

module.exports = route
