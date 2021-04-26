const { Path, email } = require('../models')
const response = require('../helpers/response')
const fs = require('fs')
const mailer = require('../helpers/mailer')

module.exports = {
  showDokumen: async (req, res) => {
    try {
      const id = req.params.id
      const result = await Path.findByPk(id)
      if (result) {
        const filePath = result.path
        fs.readFile(filePath, function (err, data) {
          if (err) {
            console.log(err)
          }
          res.contentType('application/pdf')
          res.send(data)
        })
      } else {
        return response(res, "can't show document", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  reminder: async (req, res) => {
    try {
      const result = await email.findAll()
      if (result) {
        const email = []
        result.map(item => {
          return (
            email.push(item.email_sa_kasir)
          )
        })
        const mailOptions = {
          from: 'no-replyaccounting@pinusmerahabadi.co.id',
          replyTo: 'no-replyaccounting@pinusmerahabadi.co.id',
          to: `${email.map(item => { return (item + ',') })}`,
          subject: 'Reminder Web Accounting',
          html: `<body>
                    <div style="margin-top: 20px; margin-bottom: 35px;">Dear Bapak/Ibu Area</div>
                    <div style="margin-bottom: 5px;">Untuk Dokumen yang direject atau yang belum lengkap dimohon untuk segera diupload.</div>
                    <div style="margin-bottom: 20px;">Pengiriman dokumen paling lambat adalah pukul 16.00 WIB.</div>
                    <div style="margin-bottom: 30px;">Best Regard,</div>
                    <div>Team Accounting</div>
                </body>`
        }
        email.map(item => {
          return (
            console.log(item + (','))
          )
        })
        mailer.sendMail(mailOptions, (error, result) => {
          if (error) {
            return response(res, 'failed to send email', { error: error }, 401, false)
          } else if (result) {
            return response(res, 'success send email', { result: result })
          }
        })
      } else {
        console.log('failed')
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
