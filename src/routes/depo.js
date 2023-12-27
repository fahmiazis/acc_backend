const route = require('express').Router()
const depo = require('../controllers/depo')

route.post('/add', depo.createDepo)
route.get('/get', depo.getDepo)
route.patch('/update/:id', depo.updateDepo)
route.delete('/delete/:id', depo.deleteDepo)
route.get('/detail/:id', depo.getDetailDepo)
route.post('/master', depo.uploadMasterDepo)
route.get('/export', depo.exportSqlDepo)

module.exports = route
