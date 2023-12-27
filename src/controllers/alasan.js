const { alasan, sequelize } = require('../models')
const response = require('../helpers/response')
const { pagination } = require('../helpers/pagination')
const joi = require('joi')
const { Op, QueryTypes } = require('sequelize')
const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const uploadMaster = require('../helpers/uploadMaster')
const fs = require('fs')
const excel = require('exceljs')
const vs = require('fs-extra')
const { APP_URL } = process.env

module.exports = {
  createAlasan: async (req, res) => {
    try {
      const level = req.user.level
      const schema = joi.object({
        kode_alasan: joi.string().required(),
        alasan: joi.string().required(),
        status: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          const result = await alasan.findAll({ where: { alasan: results.alasan } })
          if (result.length > 0) {
            return response(res, 'alasan already exist', {}, 404, false)
          } else {
            const result = await alasan.findAll({ where: { kode_alasan: results.kode_alasan } })
            if (result.length > 0) {
              return response(res, 'kode alasan already use', {}, 404, false)
            } else {
              const result = await alasan.create(results)
              if (result) {
                return response(res, 'sucessfully created alasan', { result })
              } else {
                return response(res, 'failed to create alasan', {}, 404, false)
              }
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
  updateAlasan: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      const schema = joi.object({
        kode_alasan: joi.string(),
        alasan: joi.string(),
        status: joi.string()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          if (results.alasan) {
            const result = await alasan.findAll({
              where: {
                [Op.and]: [
                  { alasan: results.alasan },
                  { kode_alasan: results.kode_alasan }
                ],
                [Op.not]: { id: id }
              }
            })
            if (result.length > 0) {
              return response(res, 'Alasan already exist', {}, 404, false)
            } else {
              if (results.kode_alasan) {
                const result = await alasan.findAll({
                  where: {
                    kode_alasan: results.kode_alasan,
                    [Op.not]: { id: id }
                  }
                })
                if (result.length > 0) {
                  return response(res, 'kode alasan already use', {}, 404, false)
                } else {
                  const result = await alasan.findByPk(id)
                  if (result) {
                    await result.update(results)
                    return response(res, 'update alasan successfully', { result })
                  } else {
                    return response(res, 'failed to update alasan', {}, 404, false)
                  }
                }
              } else {
                const result = await alasan.findByPk(id)
                if (result) {
                  await result.update(results)
                  return response(res, 'update alasan successfully', { result })
                } else {
                  return response(res, 'failed to update alasan', {}, 404, false)
                }
              }
            }
          } else {
            const result = await alasan.findByPk(id)
            if (result) {
              await result.update(results)
              return response(res, 'update alasan successfully', { result })
            } else {
              return response(res, 'failed to update alasan', {}, 404, false)
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
  deleteAlasan: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      if (level === 1) {
        const result = await alasan.findByPk(id)
        if (result) {
          await result.destroy()
          return response(res, 'succesfully delete alasan')
        } else {
          return response(res, `alasan with id ${id} not found`, {}, 404, false)
        }
      } else {
        return response(res, "you're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getAlasan: async (req, res) => {
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
      const result = await alasan.findAndCountAll({
        where: {
          [Op.or]: [
            { kode_alasan: { [Op.like]: `%${searchValue}%` } },
            { alasan: { [Op.like]: `%${searchValue}%` } }
          ]
        },
        order: [[sortValue, 'ASC']],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/alasan/get', req.query, page, limit, result.count)
      if (result) {
        return response(res, 'list alasan', { result, pageInfo })
      } else {
        return response(res, 'failed to get alasan', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailAlasan: async (req, res) => {
    try {
      const id = req.params.id
      const result = await alasan.findByPk(id)
      if (result) {
        return response(res, `succesfully get alasan with id ${id}`, { result })
      } else {
        return response(res, 'faileed to get alasan', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadMasterAlasan: async (req, res) => {
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
          const cek = ['Kode Alasan', 'Nama Alasan']
          const valid = rows[0]
          for (let i = 0; i < cek.length; i++) {
            if (valid[i] === cek[i]) {
              count.push(1)
            }
          }
          if (count.length === cek.length) {
            const plant = []
            const kode = []
            const status = []
            for (let i = 1; i < rows.length; i++) {
              const a = rows[i]
              plant.push(`Kode Alasan ${a[0]} dan Alasan ${a[1]}`)
              kode.push(`${a[0]}`)
              status.push(`${a[1]}`)
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
                const select = await sequelize.query(`SELECT kode_alasan, alasan from alasans WHERE kode_alasan='${kode[i]}'`, {
                  type: QueryTypes.SELECT
                })
                await sequelize.query(`DELETE from alasans WHERE kode_alasan='${kode[i]}'`, {
                  type: QueryTypes.DELETE
                })
                if (select.length > 0) {
                  arr.push(select[0])
                }
              }
              if (arr.length > 0) {
                rows.shift()
                const result = await sequelize.query(`INSERT INTO alasans (kode_alasan, alasan) VALUES ${rows.map(a => '(?)').join(',')}`,
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
                const result = await sequelize.query(`INSERT INTO alasans (kode_alasan, alasan) VALUES ${rows.map(a => '(?)').join(',')}`,
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
  exportSqlAlasan: async (req, res) => {
    try {
      const result = await alasan.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        worksheet.columns = [
          { header: 'Kode Alasan', key: 'kode_alasan', width: 10 },
          { header: 'Nama Alasan', key: 'alasan', width: 10 }
        ]
        const cek = worksheet.addRows(result)
        if (cek) {
          const name = new Date().getTime().toString().concat('-alasan').concat('.xlsx')
          await workbook.xlsx.writeFile(name)
          vs.move(name, `assets/exports/${name}`, function (err) {
            if (err) {
              throw err
            }
            console.log('success')
          })
          return response(res, 'success', { link: `${APP_URL}/download/${name}` })
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
