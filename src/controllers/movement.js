const { movement } = require('../models')
const joi = require('joi')
const { Op } = require('sequelize')
const response = require('../helpers/response')
const fs = require('fs')
const { pagination } = require('../helpers/pagination')
const uploadMaster = require('../helpers/uploadMaster')
const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const excel = require('exceljs')
const vs = require('fs-extra')
const { APP_URL } = process.env
const borderStyles = {
  top: { style: 'thin' },
  left: { style: 'thin' },
  bottom: { style: 'thin' },
  right: { style: 'thin' }
}

module.exports = {
  addMovement: async (req, res) => {
    try {
      const schema = joi.object({
        mv_type: joi.string().required(),
        mv_text: joi.string().required(),
        mv_grouping: joi.string().required(),
        comp_grouping: joi.string().required(),
        storage_loc: joi.string().required(),
        saldo: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 404, false)
      } else {
        const findMovement = await movement.findOne({
          where: {
            [Op.and]: [
                { mv_type: results.mv_type },
                { mv_text: results.mv_text },
                { storage_loc: results.storage_loc }
            ]
          }
        })
        if (findMovement) {
          return response(res, 'movement telah terdftar', {}, 404, false)
        } else {
          const createMovement = await movement.create(results)
          if (createMovement) {
            return response(res, 'success create movement')
          } else {
            return response(res, 'false create movement', {}, 404, false)
          }
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  updateMovement: async (req, res) => {
    try {
      const id = req.params.id
      const schema = joi.object({
        mv_type: joi.string().required(),
        mv_text: joi.string().required(),
        mv_grouping: joi.string().required(),
        comp_grouping: joi.string().required(),
        storage_loc: joi.string().required(),
        saldo: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 404, false)
      } else {
        const findMovement = await movement.findOne({
          where: {
            [Op.and]: [
                { mv_type: results.mv_type },
                { mv_text: results.mv_text },
                { storage_loc: results.storage_loc }
            ],
            [Op.not]: {
              id: id
            }
          }
        })
        if (findMovement) {
          return response(res, 'movement telah terdftar', {}, 404, false)
        } else {
          const findMovement = await movement.findByPk(id)
          if (findMovement) {
            const updateMovement = await findMovement.update(results)
            if (updateMovement) {
              return response(res, 'success create movement')
            } else {
              return response(res, 'false create movement', {}, 404, false)
            }
          } else {
            return response(res, 'false create movement', {}, 404, false)
          }
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadMasterMovement: async (req, res) => {
    const level = req.user.level // eslint-disable-line
    // if (level === 1) {
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
        const cek = ['Movement type', 'Movement Type Text', 'Grouping Arus Barang', 'Grouping Compare', 'Storage location', 'Saldo']
        const valid = rows[0]
        for (let i = 0; i < cek.length; i++) {
          console.log(valid[i] === cek[i])
          if (valid[i] === cek[i]) {
            count.push(1)
          }
        }
        console.log(count.length)
        if (count.length === cek.length) {
          const cost = []
          const kode = []
          for (let i = 1; i < rows.length; i++) {
            const a = rows[i]
            kode.push(`${a[0]}`)
            cost.push(`Movement type ${a[0]} Movement Type Text ${a[1]} Storage location ${a[4]}`)
          }
          const result = []
          const dupCost = {}

          cost.forEach(item => {
            if (!dupCost[item]) { dupCost[item] = 0 }
            dupCost[item] += 1
          })

          for (const prop in dupCost) {
            if (dupCost[prop] >= 2) {
              result.push(prop)
            }
          }

          // if (result.length > 0) {
          //   return response(res, 'there is duplication in your file master', { result }, 404, false)
          // } else {
          const arr = []
          rows.shift()
          for (let i = 0; i < rows.length; i++) {
            const dataMovement = rows[i]
            const select = await movement.findOne({
              where: {
                [Op.and]: [
                  { mv_type: dataMovement[0] },
                  { mv_text: dataMovement[1] },
                  { storage_loc: dataMovement[4] }
                ]
              }
            })
            const data = {
              mv_type: dataMovement[0],
              mv_text: dataMovement[1],
              mv_grouping: dataMovement[2],
              comp_grouping: dataMovement[3],
              storage_loc: dataMovement[4],
              saldo: dataMovement[5]
            }
            if (select) {
              const upbank = await select.update(data)
              if (upbank) {
                arr.push(1)
              }
            } else {
              const createMovement = await movement.create(data)
              if (createMovement) {
                arr.push(1)
              }
            }
          }
          if (arr.length > 0) {
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
          // }
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
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  },
  getMovement: async (req, res) => {
    try {
      // const kode = req.user.kode
      const findMovement = await movement.findAll()
      if (findMovement.length > 0) {
        return response(res, 'succes get movement', { result: findMovement, length: findMovement.length })
      } else {
        return response(res, 'failed get movement', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getAllMovement: async (req, res) => {
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
        sortValue = sort || 'id'
      }
      if (!limit) {
        limit = 10
      } else if (limit === 'all') {
        const findLimit = await movement.findAll()
        limit = findLimit.length
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const findMovement = await movement.findAndCountAll({
        where: {
          [Op.or]: [
            { mv_type: { [Op.like]: `%${searchValue}%` } },
            { mv_text: { [Op.like]: `%${searchValue}%` } },
            { mv_grouping: { [Op.like]: `%${searchValue}%` } },
            { comp_grouping: { [Op.like]: `%${searchValue}%` } },
            { storage_loc: { [Op.like]: `%${searchValue}%` } },
            { saldo: { [Op.like]: `%${searchValue}%` } }
          ]
        },
        order: [[sortValue, 'ASC']],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/movement/get', req.query, page, limit, findMovement.count)
      if (findMovement) {
        return response(res, 'succes get movement', { result: findMovement, pageInfo })
      } else {
        return response(res, 'failed get movement', { result: [], pageInfo })
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailMovement: async (req, res) => {
    try {
      const id = req.params.id
      const findMovement = await movement.findByPk(id)
      if (findMovement) {
        return response(res, 'succes get detail movement', { result: findMovement })
      } else {
        return response(res, 'failed get movement', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deleteMovement: async (req, res) => {
    try {
      const level = req.user.level
      const { listId } = req.body
      console.log(req.body)
      // if (level === 1) {
        if (listId !== undefined && listId.length > 0) {
          const cekData = []
          for (let i = 0; i < listId.length; i++) {
            const result = await movement.findByPk(listId[i])
            if (result) {
              await result.destroy()
              cekData.push(result)
            }
          }
          if (cekData.length > 0) {
            return response(res, 'success delete movement', { result: cekData })
          } else {
            return response(res, 'movement not found', {}, 404, false)
          }
        } else {
          return response(res, 'movement not found', {}, 404, false)
        }
      // } else {
      //   return response(res, "You're not super administrator", {}, 404, false)
      // }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  exportSqlMovement: async (req, res) => {
    try {
      const result = await movement.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        const arr = []
        const header = ['Movement type', 'Movement Type Text', 'Grouping Arus Barang', 'Grouping Compare', 'Storage location', 'Saldo']
        const key = ['mv_type', 'mv_text', 'mv_grouping', 'comp_grouping', 'storage_loc', 'saldo']
        for (let i = 0; i < header.length; i++) {
          let temp = { header: header[i], key: key[i] }
          arr.push(temp)
          temp = {}
        }
        worksheet.columns = arr
        worksheet.addRows(result)
        worksheet.eachRow({ includeEmpty: true }, function (row, rowNumber) {
          row.eachCell({ includeEmpty: true }, function (cell, colNumber) {
            cell.border = borderStyles
          })
        })

        worksheet.columns.forEach(column => {
          const lengths = column.values.map(v => v.toString().length)
          const maxLength = Math.max(...lengths.filter(v => typeof v === 'number'))
          column.width = maxLength + 5
        })
        const cek = [1]
        if (cek.length > 0) {
          const name = new Date().getTime().toString().concat('-movement').concat('.xlsx')
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
  },
  deleteAll: async (req, res) => {
    try {
      const findMovement = await movement.findAll()
      if (findMovement) {
        const temp = []
        for (let i = 0; i < findMovement.length; i++) {
          const findDel = await movement.findByPk(findMovement[i].id)
          if (findDel) {
            await findDel.destroy()
            temp.push(1)
          }
        }
        if (temp.length > 0) {
          return response(res, 'success delete all', {}, 404, false)
        } else {
          return response(res, 'failed delete all', {}, 404, false)
        }
      } else {
        return response(res, 'failed delete all', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
