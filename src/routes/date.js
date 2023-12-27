const route = require('express').Router()
const date = require('../controllers/dateClossing')

route.post('/create', date.addDateClossing)
route.get('/get', date.getClossing)

module.exports = route
