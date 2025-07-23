const nodemailer = require('nodemailer')
// const { HOST, PORT, USER, PASS } = process.env

const transporter = nodemailer.createTransport({
  host: '192.168.35.203',
  secure: false,
  port: 587,
  auth: {
    user: 'sys_adm@pinusmerahabadi.co.id',
    pass: 'pma159753'
  },
  tls: {
    rejectUnauthorized: false
  }
})

module.exports = transporter
