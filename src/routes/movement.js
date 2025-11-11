const route = require('express').Router()
const movement = require('../controllers/movement')

route.post('/add', movement.addMovement)
route.get('/all', movement.getMovement)
route.get('/get', movement.getAllMovement)
route.get('/detail/:no', movement.getDetailMovement)
route.patch('/update/:id', movement.updateMovement)
route.post('/master', movement.uploadMasterMovement)
route.patch('/delete', movement.deleteMovement)
route.delete('/delall', movement.deleteAll)
route.get('/export', movement.exportSqlMovement)

module.exports = route
