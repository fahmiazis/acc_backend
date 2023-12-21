const multer = require('multer')

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    if (!file) {
      return cb(new Error('document cant be null'), false)
    }
    cb(null, 'assets/masters')
  },
  filename: (req, file, cb) => {
    const ext = file.originalname.split('.')[file.originalname.split('.').length - 1]
    const fileName = req.params.id ? `${req.params.id}_${new Date().getTime().toString().concat('.').concat(ext)}` : `${new Date().getTime().toString().concat('.').concat(ext)}`
    cb(null, fileName)
  }
})

const fileFilter = (req, file, cb) => {
  const allowedMimes = ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']
  if (allowedMimes.includes(file.mimetype)) {
    return cb(null, true)
  }
  return cb(new Error('Invalid file type. Only excel files are allowed.'), false)
}

module.exports = multer({ storage, fileFilter, limits: { fileSize: 5000000 } }).array('master', 1)
