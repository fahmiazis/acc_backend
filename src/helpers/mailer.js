const nodemailer = require('nodemailer')
// const { HOST, PORT, USER, PASS } = process.env

const transporter = nodemailer.createTransport({
  host: 'mail.pinusmerahabadi.co.id',
  secure: false,
  port: 587,
  auth: {
    user: 'sys_adm@pinusmerahabadi.co.id',
    pass: 'sys0911'
  },
  tls: {
    rejectUnauthorized: false
  }
})

module.exports = transporter
