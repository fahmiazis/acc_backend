const route = require('express').Router()
const auth = require('../controllers/auth')

route.post('/login', auth.login)
route.post('/register', auth.register)

module.exports = route
