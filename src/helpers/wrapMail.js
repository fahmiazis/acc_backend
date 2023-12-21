const nodemailer = require('nodemailer')
// const { HOST, PORT, USER, PASS } = process.env
async function wrapedSendMail (mailOptions) {
  return new Promise((resolve, reject) => {
    const transporter = nodemailer.createTransport({
      connectionTimeout: 60000,
      socketTimeout: 120000,
      greetingTimeout: 30000,
      host: 'mail.pinusmerahabadi.co.id',
      secure: false,
      port: 587,
      auth: {
        user: 'acc@pinusmerahabadi.co.id',
        pass: 'acc1223'
      },
      tls: {
        rejectUnauthorized: false
      }
      // service: 'gmail',
      // auth: {
      //   user: 'insfopma@gmail.com',
      //   pass: 'dwnf ykro lbml pbvn'
      // }
    })
    let cek = []
    transporter.sendMail(mailOptions, function (error, info) {
      if (error) {
        console.log('error is ' + error)
        resolve(false)
      } else {
        console.log('Email sent: ' + info.response)
        cek.push(1)
        resolve(true)
      }
    })
    setTimeout(() => {
      transporter.close()
      if (cek.length > 0) {
        cek = []
        console.log('masuk settimeout true')
        resolve(true)
      } else {
        cek = []
        console.log('masuk settimeout false')
        resolve(true)
      }
    }, 10000)
  })
}

module.exports = { wrapedSendMail }
