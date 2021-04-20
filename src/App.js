const express = require('express')
const bodyParser = require('body-parser')
const cors = require('cors')
const response = require('./helpers/response')
const request = require('request')
// const fs = require('fs')
const cron = require('node-cron')

const app = express()
const server = require('http').createServer(app)

const { APP_PORT, APP_URL } = process.env

app.use(bodyParser.urlencoded({ extended: false }))
app.use(cors())

const authMiddleware = require('./middleware/auth')

const userRoute = require('./routes/user')
const authRoute = require('./routes/auth')
const alasanRoute = require('./routes/alasan')
const divisiRoute = require('./routes/divisi')
const emailRoute = require('./routes/email')
const dokumenRoute = require('./routes/dokumen')
const depoRoute = require('./routes/depo')
const picRoute = require('./routes/pic')
const transRoute = require('./routes/transaction')
const showRoute = require('./routes/show')

app.use('/uploads', express.static('assets/documents'))
app.use('/masters', express.static('assets/masters'))
app.use('/download', express.static('assets/exports'))

app.use('/auth', authRoute)
app.use('/dashboard', authMiddleware, transRoute)
app.use('/user', authMiddleware, userRoute)
app.use('/alasan', authMiddleware, alasanRoute)
app.use('/divisi', authMiddleware, divisiRoute)
app.use('/email', authMiddleware, emailRoute)
app.use('/dokumen', authMiddleware, dokumenRoute)
app.use('/depo', authMiddleware, depoRoute)
app.use('/pic', authMiddleware, picRoute)
app.use('/show', showRoute)

const options = {
  method: 'GET',
  url: 'http://localhost:7575/show/reminder'
}

cron.schedule('0 16 * * *', () => {
  request(options, function (error, response, body) {
    if (error) {
      console.log(error)
    }
  })
}, {
  scheduled: true,
  timezone: 'Asia/Jakarta'
})

// app.get('/show', function (req, res) {
//   const filePath = 'assets/documents/1_1617886935906.pdf'

//   fs.readFile(filePath, function (err, data) {
//     if (err) {
//       console.log(err)
//     }
//     res.contentType('application/pdf')
//     res.send(data)
//   })
// })

app.get('*', (req, res) => {
  response(res, 'Error route not found', {}, 404, false)
})

server.listen(APP_PORT, () => {
  console.log(`App is running on port ${APP_URL}`)
})
