const route = require('express').Router()
const user = require('../controllers/user')

route.post('/add', user.addUser)
route.get('/get', user.getUsers)
route.patch('/update/:id', user.updateUser)
route.patch('/delete', user.deleteUser)
route.get('/detail/:id', user.getDetailUser)
route.post('/master', user.uploadMasterUser)
route.get('/export', user.exportSqlUser)
route.get('/create/pic', user.createUserPic)
route.get('/create/spv', user.createUserSpv)

module.exports = route
