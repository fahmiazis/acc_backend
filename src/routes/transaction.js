const route = require('express').Router()
const trans = require('../controllers/transaction')

route.get('/get', trans.dashboard)
route.get('/activity', trans.getActivity)
route.post('/upload/:id/:idAct', trans.uploadDocument)
route.patch('/upload/edit/:id/:idAct', trans.editUploadDocument)
route.patch('/approve/:id/:idAct', trans.approveDocument)
route.patch('/reject/:id/:idAct', trans.rejectDocument)
route.post('/send', trans.sendMail)
route.post('/sendarea', trans.sendMailArea)
route.post('/report', trans.reportDokumen)
route.get('/active', trans.getAllActivity)
route.patch('/edit/:id', trans.editAccessActive)
route.get('/notif', trans.getNotif)

module.exports = route
