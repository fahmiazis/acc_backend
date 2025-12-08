const joi = require('joi')
const { divisi, sequelize } = require('../models')
const { Op, QueryTypes } = require('sequelize')
const { pagination } = require('../helpers/pagination')
const response = require('../helpers/response')
const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const uploadMaster = require('../helpers/uploadMaster')
const fs = require('fs')
const excel = require('exceljs')
const vs = require('fs-extra')
const { APP_BE } = process.env

module.exports = {
  createDivisi: async (req, res) => {
    try {
      const level = req.user.level
      const schema = joi.object({
        divisi: joi.string().required(),
        status: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          const result = await divisi.findAll({ where: { divisi: results.divisi } })
          if (result.length > 0) {
            return response(res, 'division already exists', {}, 404, false)
          } else {
            const result = await divisi.create(results)
            if (result) {
              return response(res, 'division created', { result })
            } else {
              return response(res, 'failed to create division', {}, 404, false)
            }
          }
        } else {
          return response(res, "you're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      response(res, error.message, {}, 500, false)
    }
  },
  updateDivisi: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      const schema = joi.object({
        divisi: joi.string(),
        status: joi.string()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          const result = await divisi.findAll({
            where: {
              divisi: results.divisi,
              [Op.not]: { id: id }
            }
          })
          if (result.length > 0) {
            return response(res, 'divisi already exist', {}, 404, false)
          } else {
            const result = await divisi.findByPk(id)
            if (result) {
              await result.update(results)
              return response(res, 'successfully update divisi', { result })
            } else {
              return response(res, 'failed to update divisi', {}, 404, false)
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
  deleteDivisi: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      if (level === 1) {
        const result = await divisi.findByPk(id)
        if (result) {
          await result.destroy()
          return response(res, 'succesfully delete divisi')
        } else {
          return response(res, `divisi with id ${id} not found`, {}, 404, false)
        }
      } else {
        return response(res, "you're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDivisi: async (req, res) => {
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
        limit = 5
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const result = await divisi.findAndCountAll({
        where: {
          divisi: { [Op.like]: `%${searchValue}%` }
        },
        order: [[sortValue, 'ASC']],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/divisi/get', req.query, page, limit, result.count)
      if (result) {
        return response(res, 'list divisi', { result, pageInfo })
      } else {
        return response(res, 'failed to get divisi', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailDivisi: async (req, res) => {
    try {
      const id = req.params.id
      const result = await divisi.findByPk(id)
      if (result) {
        return response(res, `succesfully get divisi with id ${id}`, { result })
      } else {
        return response(res, 'faileed to get divisi', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadMasterDivisi: async (req, res) => {
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
          const cek = ['Nama Divisi']
          const valid = rows[0]
          for (let i = 0; i < cek.length; i++) {
            if (valid[i] === cek[i]) {
              console.log(`${valid[i]} === ${cek[i]}`)
              count.push(1)
            }
          }
          if (count.length === cek.length) {
            const plant = []
            const divisi = []
            for (let i = 1; i < rows.length; i++) {
              const a = rows[i]
              plant.push(`Nama Divisi ${a[0]}`)
              divisi.push(`${a[0]}`)
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
              const arr = []
              for (let i = 0; i < rows.length - 1; i++) {
                const select = await sequelize.query(`SELECT divisi from divisis WHERE divisi='${divisi[i]}'`, {
                  type: QueryTypes.SELECT
                })
                await sequelize.query(`DELETE from divisis WHERE divisi='${divisi[i]}'`, {
                  type: QueryTypes.DELETE
                })
                if (select.length > 0) {
                  console.log(select[0])
                  arr.push(select[0])
                }
              }
              console.log(arr.length)
              if (arr.length > 0) {
                rows.shift()
                const result = await sequelize.query(`INSERT INTO divisis (divisi) VALUES ${rows.map(a => '(?)').join(',')}`,
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
                const result = await sequelize.query(`INSERT INTO divisis (divisi) VALUES ${rows.map(a => '(?)').join(',')}`,
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
  exportSqlDivisi: async (req, res) => {
    try {
      const result = await divisi.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        worksheet.columns = [
          { header: 'Nama Divisi', key: 'divisi', width: 10 }
        ]
        const cek = worksheet.addRows(result)
        if (cek) {
          const name = new Date().getTime().toString().concat('-divisi').concat('.xlsx')
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
