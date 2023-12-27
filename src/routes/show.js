const route = require('express').Router()
const show = require('../controllers/show')

route.get('/get/:id', show.showDokumen)
route.get('/reminder', show.reminder)

module.exports = route
