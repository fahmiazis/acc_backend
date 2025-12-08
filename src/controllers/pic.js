const { pic, sequelize } = require('../models')
const joi = require('joi')
const response = require('../helpers/response')
const { pagination } = require('../helpers/pagination')
const { Op, QueryTypes } = require('sequelize')
const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const uploadMaster = require('../helpers/uploadMaster')
const fs = require('fs')
const excel = require('exceljs')
const vs = require('fs-extra')
const { APP_BE } = process.env

module.exports = {
  addPic: async (req, res) => {
    try {
      const level = req.user.level
      const schema = joi.object({
        pic: joi.string().required(),
        spv: joi.string().required(),
        divisi: joi.string().required(),
        kode_depo: joi.string().required().valid(),
        nama_depo: joi.string().required(),
        status: joi.string().required().valid('active', 'inactive')
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          const result = await pic.findAll({ 
            where: {
              [Op.and]: [
                { kode_depo: results.kode_depo },
                { pic: results.pic }  
              ]
            } 
          })
          if (result.length > 0) {
            return response(res, 'kode depo already use', {}, 404, false)
          } else {
            const result = await pic.create(results)
            if (result) {
              return response(res, 'succesfully add pic', { result })
            } else {
              return response(res, 'failed to add pic', {}, 404, false)
            }
          }
        } else {
          return response(res, "you're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  updatePic: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      const schema = joi.object({
        pic: joi.string(),
        spv: joi.string(),
        divisi: joi.string(),
        kode_depo: joi.string(),
        nama_depo: joi.string(),
        status: joi.string()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          if (results.kode_depo) {
            const result = await pic.findAll({
              where: {
                [Op.and]: [
                  { kode_depo: results.kode_depo },
                  { pic: results.pic }  
                ],
                [Op.not]: { id: id }
              }
            })
            if (result.length > 0) {
              return response(res, 'kode depo already use', {}, 404, false)
            } else {
              const result = await pic.findByPk(id)
              if (result) {
                await result.update(results)
                return response(res, 'successfully update pic', { result })
              } else {
                return response(res, 'failed to update pic', {}, 404, false)
              }
            }
          } else {
            const result = await pic.findByPk(id)
            if (result) {
              await result.update(results)
              return response(res, 'successfully update pic', { result })
            } else {
              return response(res, 'failed to update pic', {}, 404, false)
            }
          }
        } else {
          return response(res, "you're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getPic: async (req, res) => {
    try {
      let { limit, page, search, sort } = req.query
      let searchValue = ''
      let sortValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || ''
      }
      if (typeof sort === 'object') {
        sortValue = Object.values(sort)[0]
      } else {
        sortValue = sort || 'createdAt'
      }
      if (!limit) {
        limit = 10
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const result = await pic.findAndCountAll({
        where: {
          [Op.or]: [
            { pic: { [Op.like]: `%${searchValue}%` } },
            { kode_depo: { [Op.like]: `%${searchValue}%` } },
            { spv: { [Op.like]: `%${searchValue}%` } },
            { divisi: { [Op.like]: `%${searchValue}%` } },
            { nama_depo: { [Op.like]: `%${searchValue}%` } },
            { status: { [Op.like]: `%${searchValue}%` } }
          ]
        },
        order: [[sortValue, 'ASC']],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/pic/get', req.query, page, limit, result.count)
      if (result) {
        return response(res, 'list pic', { result, pageInfo })
      } else {
        return response(res, 'failed to get pic', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailPic: async (req, res) => {
    try {
      const id = req.params.id
      const schema = joi.object({
        kode_depo: joi.string()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (results.kode_depo) {
          const result = await pic.findAll({ where: { kode_depo: results.kode_depo } })
          if (result) {
            return response(res, `succesfully get pic with kode depo ${results.kode_depo}`, { result })
          } else {
            return response(res, 'pic not found', {}, 404, false)
          }
        } else {
          const result = await pic.findByPk(id)
          if (result) {
            return response(res, `succesfully get pic with id ${id}`, { result })
          } else {
            return response(res, 'pic not found', {}, 404, false)
          }
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deletePic: async (req, res) => {
    try {
      const level = req.user.level
      const { listId } = req.body
      console.log(req.body)
      if (level === 1) {
        if (listId !== undefined && listId.length > 0) {
          const cekData = []
          for (let i = 0; i < listId.length; i++) {
            const result = await pic.findByPk(listId[i])
            if (result) {
              await result.destroy()
              cekData.push(result)
            }
          }
          if (cekData.length > 0) {
            return response(res, 'success delete pic', { result: cekData })
          } else {
            return response(res, 'pic not found', {}, 404, false)
          }
        } else {
          return response(res, 'pic not found', {}, 404, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadMasterPic: async (req, res) => {
    const level = req.user.level
    if (level === 1) {
      uploadMaster(req, res, async function (err) {
        try {
          if (err instanceof multer.MulterError) {
            if (err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length === 0) {
              console.log(err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length > 0)
              return response(res, 'fieldname doesnt match', {}, 500, false)
            }
            return response(res, err.message, {}, 500, false)
          } else if (err) {
            return response(res, err.message, {}, 401, false)
          }
          const dokumen = `assets/masters/${req.files[0].filename}`
          const rows = await readXlsxFile(dokumen)
          const count = []
          const cek = ['Nama PIC', 'Nama SPV', 'Divisi', 'Kode Depo', 'Nama Depo']
          const valid = rows[0]
          for (let i = 0; i < cek.length; i++) {
            if (valid[i] === cek[i]) {
              count.push(1)
            }
          }
          if (count.length === cek.length) {
            const plant = []
            const pic1 = []
            const kode = []
            for (let i = 1; i < rows.length; i++) {
              const a = rows[i]
              plant.push(`Nama PIC ${a[0]} dan Kode Depo ${a[3]}`)
              pic1.push(`${a[0]}`)
              kode.push(`${a[3]}`)
            }
            const object = {}
            const result = []

            plant.forEach(item => {
              if (!object[item]) { object[item] = 0 }
              object[item] += 1
            })

            for (const prop in object) {
              if (object[prop] >= 2) {
                result.push(prop)
              }
            }

            if (result.length > 0) {
              return response(res, 'there is duplication in your file master', { result }, 404, false)
            } else {
              const findDoc = await pic.findAll()
              if (findDoc.length > 0 || findDoc.length === 0) {
                const arr = []
                for (let i = 0; i < findDoc.length; i++) {
                  const findId = await pic.findByPk(findDoc[i].id)
                  if (findId) {
                    await findId.destroy()
                    arr.push(findId)
                  }
                }
                if (arr.length > 0) {
                  rows.shift()
                  const result = await sequelize.query(`INSERT INTO pics (pic, spv, divisi, kode_depo, nama_depo) VALUES ${rows.map(a => '(?)').join(',')}`,
                    {
                      replacements: rows,
                      type: QueryTypes.INSERT
                    })
                  if (result) {
                    fs.unlink(dokumen, function (err) {
                      if (err) throw err
                      console.log('success')
                    })
                    return response(res, 'successfully upload file master')
                  } else {
                    fs.unlink(dokumen, function (err) {
                      if (err) throw err
                      console.log('success')
                    })
                    return response(res, 'failed to upload file', {}, 404, false)
                  }
                } else {
                  rows.shift()
                  const result = await sequelize.query(`INSERT INTO pics (pic, spv, divisi, kode_depo, nama_depo) VALUES ${rows.map(a => '(?)').join(',')}`,
                    {
                      replacements: rows,
                      type: QueryTypes.INSERT
                    })
                  if (result) {
                    fs.unlink(dokumen, function (err) {
                      if (err) throw err
                      console.log('success')
                    })
                    return response(res, 'successfully upload file master')
                  } else {
                    fs.unlink(dokumen, function (err) {
                      if (err) throw err
                      console.log('success')
                    })
                    return response(res, 'failed to upload file', {}, 404, false)
                  }
                }
              }
            }
          } else {
            fs.unlink(dokumen, function (err) {
              if (err) throw err
              console.log('success')
            })
            return response(res, 'Failed to upload master file, please use the template provided', {}, 400, false)
          }
        } catch (error) {
          return response(res, error.message, {}, 500, false)
        }
      })
    } else {
      return response(res, "You're not super administrator", {}, 404, false)
    }
  },
  exportSqlPic: async (req, res) => {
    try {
      const result = await pic.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        const arr = []
        const header = ['Nama PIC', 'Nama SPV', 'Divisi', 'Kode Depo', 'Nama Depo']
        const key = ['pic', 'spv', 'divisi', 'kode_depo', 'nama_depo']
        for (let i = 0; i < header.length; i++) {
          let temp = { header: header[i], key: key[i] }
          arr.push(temp)
          temp = {}
        }
        worksheet.columns = arr
        const cek = worksheet.addRows(result)
        if (cek) {
          const name = new Date().getTime().toString().concat('-pic').concat('.xlsx')
          await workbook.xlsx.writeFile(name)
          vs.move(name, `assets/exports/${name}`, function (err) {
            if (err) {
              throw err
            }
            console.log('success')
          })
          return response(res, 'success', { link: `${APP_BE}/download/${name}` })
        } else {
          return response(res, 'failed create file', {}, 404, false)
        }
      } else {
        return response(res, 'failed', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
