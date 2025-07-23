const express = require('express')
const bodyParser = require('body-parser')
const cors = require('cors')
const response = require('./helpers/response')
const request = require('request')
// const fs = require('fs')
const morgan = require('morgan')
const cron = require('node-cron')
// const moment = require('moment')

const app = express()
const server = require('http').createServer(app)
// const io = require('socket.io')(server, {
//   cors: {
//     origin: 'http://localhost:7575',
//     methods: ['GET', 'POST'],
//     allowedHeaders: ['Authorization'],
//     credentials: true
//   }
// })
// module.exports = io

const { APP_PORT, APP_URL } = process.env

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(morgan('dev'))
app.use(cors())
// app.use(setTimeout(14400000))

const userRoute = require('./routes/user')
const authRoute = require('./routes/auth')
const alasanRoute = require('./routes/alasan')
const divisiRoute = require('./routes/divisi')
const dateRoute = require('./routes/date')
const emailRoute = require('./routes/email')
const dokumenRoute = require('./routes/dokumen')
const depoRoute = require('./routes/depo')
const picRoute = require('./routes/pic')
const transRoute = require('./routes/transaction')
const showRoute = require('./routes/show')

// upload tax sales
const datamergeRoute = require('./routes/datamerge')

const authMiddleware = require('./middleware/auth')

app.use('/uploads', express.static('assets/documents'))
app.use('/masters', express.static('assets/masters'))
app.use('/merge', express.static('assets/merge'))
app.use('/download', express.static('assets/exports'))

app.use('/auth', authRoute)
app.use('/dashboard', authMiddleware, transRoute)
app.use('/user', authMiddleware, userRoute)
app.use('/date', authMiddleware, dateRoute)
app.use('/alasan', authMiddleware, alasanRoute)
app.use('/divisi', authMiddleware, divisiRoute)
app.use('/email', authMiddleware, emailRoute)
app.use('/dokumen', authMiddleware, dokumenRoute)
app.use('/depo', authMiddleware, depoRoute)
app.use('/pic', authMiddleware, picRoute)
app.use('/show', showRoute)

// upload sales tax
app.use('/datamerge', datamergeRoute)

const options = {
  method: 'GET',
  url: 'http://192.168.35.163:7575/show/reminder'
}

cron.schedule('0 12,15 * * 1-5', () => {
  request(options, function (error, response, body) {
    if (error) {
      console.log(error)
    }
  })
}, {
  scheduled: true,
  timezone: 'Asia/Jakarta'
})

// cron.schedule('0 12,15 * * 1-5', () => {
//   console.log('mantap' + moment().format('LLL'))
// }, {
//   scheduled: true,
//   timezone: 'Asia/Jakarta'
// })

app.get('*', (req, res) => {
  response(res, 'Error route not found', {}, 404, false)
})

app.get('/', (req, res) => {
  res.send({
    success: true,
    message: 'Backend is running'
  })
})

server.listen(APP_PORT, () => {
  console.log(`App is running on port ${APP_URL}`)
})
