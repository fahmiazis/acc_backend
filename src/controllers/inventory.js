const { inventory, report_inven, movement } = require('../models')
const joi = require('joi')
const { Op } = require('sequelize')
const response = require('../helpers/response')
const fs = require('fs')
const { pagination } = require('../helpers/pagination')
const uploadMaster = require('../helpers/uploadMaster')
const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const moment = require('moment')
const excel = require('exceljs')
const vs = require('fs-extra')
const { spawn } = require('child_process')
const path = require('path')
const { APP_BE } = process.env
const pythonPathLocal = 'python'
const pythonPathProd = '/usr/bin/python'
const borderStyles = {
  top: { style: 'thin' },
  left: { style: 'thin' },
  bottom: { style: 'thin' },
  right: { style: 'thin' }
}

module.exports = {
  addInventory: async (req, res) => {
    try {
      const schema = joi.object({
        plant: joi.string().required(),
        area: joi.string().required(),
        channel: joi.string().required(),
        profit_center: joi.string().required(),
        kode_dist: joi.string().required(),
        pic_inv: joi.string().required(),
        pic_kasbank: joi.string().required(),
        status_area: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 404, false)
      } else {
        const findInventory = await inventory.findOne({
          where: {
            plant: results.plant
          }
        })
        if (findInventory) {
          return response(res, 'inventory telah terdftar', {}, 404, false)
        } else {
          const createInventory = await inventory.create(results)
          if (createInventory) {
            return response(res, 'success create inventory')
          } else {
            return response(res, 'false create inventory', {}, 404, false)
          }
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  updateInventory: async (req, res) => {
    try {
      const id = req.params.id
      const schema = joi.object({
        plant: joi.string().required(),
        area: joi.string().required(),
        channel: joi.string().required(),
        profit_center: joi.string().required(),
        kode_dist: joi.string().required(),
        pic_inv: joi.string().required(),
        pic_kasbank: joi.string().required(),
        status_area: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 404, false)
      } else {
        const findInventory = await inventory.findOne({
          where: {
            plant: results.plant,
            [Op.not]: {
              id: id
            }
          }
        })
        if (findInventory) {
          return response(res, 'inventory telah terdftar', {}, 404, false)
        } else {
          const findInventory = await inventory.findByPk(id)
          if (findInventory) {
            const updateInventory = await findInventory.update(results)
            if (updateInventory) {
              return response(res, 'success create inventory')
            } else {
              return response(res, 'false create inventory', {}, 404, false)
            }
          } else {
            return response(res, 'false create inventory', {}, 404, false)
          }
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadMasterInventory: async (req, res) => {
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
        const cek = ['PLANT', 'NAMA AREA', 'CHANNEL', 'PROFIT CENTER', 'KODE DIST', 'PIC INVENTORY', 'PIC KASBANK', 'STATUS']
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
            cost.push(`Plant ${a[0]}`)
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

          if (result.length > 0) {
            return response(res, 'there is duplication in your file master', { result }, 404, false)
          } else {
            const arr = []
            rows.shift()
            for (let i = 0; i < rows.length; i++) {
              const dataInventory = rows[i]
              const select = await inventory.findOne({
                where: {
                  plant: dataInventory[0]
                }
              })
              const data = {
                plant: dataInventory[0],
                area: dataInventory[1],
                channel: dataInventory[2],
                profit_center: dataInventory[3],
                kode_dist: dataInventory[4],
                pic_inv: dataInventory[5],
                pic_kasbank: dataInventory[6],
                status_area: dataInventory[7]
              }
              if (select) {
                const upbank = await select.update(data)
                if (upbank) {
                  arr.push(1)
                }
              } else {
                const createInventory = await inventory.create(data)
                if (createInventory) {
                  arr.push(1)
                }
              }
            }
            if (arr.length > 0) {
              fs.unlink(dokumen, function (err) {
                if (err) throw err
                console.log('success')
              })
              return response(res, 'successfully upload file master', { result: level })
            } else {
              fs.unlink(dokumen, function (err) {
                if (err) throw err
                console.log('success')
              })
              return response(res, 'failed to upload file', {}, 404, false)
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
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  },
  getInventory: async (req, res) => {
    try {
      // const kode = req.user.kode
      const findInventory = await inventory.findAll()
      if (findInventory.length > 0) {
        return response(res, 'succes get inventory', { result: findInventory, length: findInventory.length })
      } else {
        return response(res, 'failed get inventory', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getAllInventory: async (req, res) => {
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
        const findLimit = await inventory.findAll()
        limit = findLimit.length
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const findInventory = await inventory.findAndCountAll({
        where: {
          [Op.or]: [
            { plant: { [Op.like]: `%${searchValue}%` } },
            { area: { [Op.like]: `%${searchValue}%` } },
            { channel: { [Op.like]: `%${searchValue}%` } },
            { profit_center: { [Op.like]: `%${searchValue}%` } },
            { kode_dist: { [Op.like]: `%${searchValue}%` } },
            { pic_inv: { [Op.like]: `%${searchValue}%` } },
            { pic_kasbank: { [Op.like]: `%${searchValue}%` } },
            { status_area: { [Op.like]: `%${searchValue}%` } }
          ]
        },
        order: [[sortValue, 'ASC']],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/inventory/get', req.query, page, limit, findInventory.count)
      if (findInventory) {
        return response(res, 'succes get inventory', { result: findInventory, pageInfo })
      } else {
        return response(res, 'failed get inventory', { result: [], pageInfo })
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailInventory: async (req, res) => {
    try {
      const id = req.params.id
      const findInventory = await inventory.findByPk(id)
      if (findInventory) {
        return response(res, 'succes get detail inventory', { result: findInventory })
      } else {
        return response(res, 'failed get inventory', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deleteInventory: async (req, res) => {
    try {
      const level = req.user.level
      const { listId } = req.body
      console.log(req.body)
      // if (level === 1) {
        if (listId !== undefined && listId.length > 0) {
          const cekData = []
          for (let i = 0; i < listId.length; i++) {
            const result = await inventory.findByPk(listId[i])
            if (result) {
              await result.destroy()
              cekData.push(result)
            }
          }
          if (cekData.length > 0) {
            return response(res, 'success delete inventory', { result: cekData })
          } else {
            return response(res, 'inventory not found', {}, 404, false)
          }
        } else {
          return response(res, 'inventory not found', {}, 404, false)
        }
      // } else {
      //   return response(res, "You're not super administrator", {}, 404, false)
      // }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  exportSqlInventory: async (req, res) => {
    try {
      const result = await inventory.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        const arr = []
        const header = ['PLANT', 'NAMA AREA', 'CHANNEL', 'PROFIT CENTER', 'KODE DIST', 'PIC INVENTORY', 'PIC KASBANK', 'STATUS']
        const key = ['plant', 'area', 'channel', 'profit_center', 'kode_dist', 'pic_inv', 'pic_kasbank', 'status_area']
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
          const name = new Date().getTime().toString().concat('-inventory').concat('.xlsx')
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
  },
  deleteAll: async (req, res) => {
    try {
      const findInventory = await inventory.findAll()
      if (findInventory) {
        const temp = []
        for (let i = 0; i < findInventory.length; i++) {
          const findDel = await inventory.findByPk(findInventory[i].id)
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
  },
  // Report Inventory
  getAllReport: async (req, res) => {
    try {
      let { limit, page, search, sort, date, type } = req.query
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
        const findLimit = await report_inven.findAll()
        limit = findLimit.length
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const startDate = moment(date).startOf('month').toDate();
      const endDate = moment(date).endOf('month').toDate();
      const findInventory = await report_inven.findAndCountAll({
        where: {
          [Op.and]: [
            {
              [Op.or]: [
                { name: { [Op.like]: `%${search}%` } },
                { type: { [Op.like]: `%${search}%` } }
              ]
            },
            {
              date_report: { [Op.between]: [startDate, endDate] }
            },
            {
              status: type
            }
          ]
        },
        order: [[sortValue, 'DESC']]
        // limit: limit,
        // offset: (page - 1) * limit
      })
      const pageInfo = pagination('/report-inv/get', req.query, page, limit, findInventory.count)
      if (findInventory) {
        return response(res, 'succes get report inv', { result: findInventory, pageInfo, startDate, endDate })
      } else {
        return response(res, 'failed get report inv', { result: [], pageInfo })
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailReport: async (req, res) => {
    try {
      const id = req.params.id
      const findInventory = await report_inven.findByPk(id)
      if (findInventory) {
        return response(res, 'succes get detail report inv', { result: findInventory })
      } else {
        return response(res, 'failed get report inv', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deleteReport: async (req, res) => {
    try {
      const level = req.user.level
      const { listId } = req.body
      console.log(req.body)
      // if (level === 1) {
        if (listId !== undefined && listId.length > 0) {
          const cekData = []
          for (let i = 0; i < listId.length; i++) {
            const result = await report_inven.findByPk(listId[i])
            if (result) {
              const path = result.path
              fs.unlink(path, function (err) {
                console.log('success')
              })
              await result.destroy()
              cekData.push(result)
            }
          }
          if (cekData.length > 0) {
            return response(res, 'success delete report inv', { result: cekData })
          } else {
            return response(res, 'report inv not found', {}, 404, false)
          }
        } else {
          return response(res, 'report inv not found', {}, 404, false)
        }
      // } else {
      //   return response(res, "You're not super administrator", {}, 404, false)
      // }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadReportInv: async (req, res) => {
    const level = req.user.level // eslint-disable-line
    const username = req.user.name
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
        const { name, type, date_report, plant } = req.body
        const dokumen = `assets/masters/${req.files[0].filename}`
        const data = {
          name: name,
          type: type,
          path: dokumen,
          status: 1,
          plant: plant,
          date_report: date_report,
          user_upload: username
        }
        const createData = await report_inven.create(data)
        if (createData) {
          return response(res, 'success upload report')
        }
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  },
  updateReportInv: async (req, res) => {
    const level = req.user.level // eslint-disable-line
    const username = req.user.name
    // if (level === 1) {
    const { type } = req.query
    if (type === 'upload') {
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
          const { name, type, date_report, plant, id } = req.body
          const dokumen = `assets/masters/${req.files[0].filename}`
          const data = {
            name: name,
            type: type,
            path: dokumen,
            plant: plant,
            date_report: date_report,
            user_upload: username
          }
          const findId = await report_inven.findByPk(id)
          if (findId) {
            const updateData = await findId.update(data)
            if (updateData) {
              return response(res, 'success upload report')
            }
          }
        } catch (error) {
          return response(res, error.message, {}, 500, false)
        }
      })
    } else {
      const { name, type, date_report, plant, id } = req.body
      console.log(name)
      const data = {
        name: name,
        type: type,
        plant: plant,
        date_report: date_report,
        user_upload: username
      }
      const findId = await report_inven.findByPk(id)
      if (findId) {
        const updateData = await findId.update(data)
        if (updateData) {
          return response(res, 'success upload report')
        }
      }
    }
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  },
  generateInventoryReport: async (req, res) => {
    try {
      const username = req.user.name
      const { listPlant, date } = req.body
      
      if (date !== undefined && listPlant !== undefined && listPlant.length > 0) {
        const cekGenerate = []
        const cekFalse = []
        const startDate = moment(date).startOf('month').toDate();
        const endDate = moment(date).endOf('month').toDate();
        
        // Ambil master data dari database SEKALI saja (di luar loop)
        const masterInventory = await inventory.findAll()
        const masterMovement = await movement.findAll()

        // Validasi master data
        if (!masterInventory || masterInventory.length === 0) {
          return response(res, 'Master Inventory kosong', {}, 400, false)
        }
        if (!masterMovement || masterMovement.length === 0) {
          return response(res, 'Master Movement kosong', {}, 400, false)
        }

        // 🔧 PERBAIKAN: Buat fungsi yang return Promise untuk setiap plant
        const processPlant = (plant) => {
          return new Promise(async (resolve, reject) => {
            try {
              const files = await report_inven.findAll({
                where: { 
                  [Op.and]: [
                    { plant: plant },
                    { date_report: { [Op.between]: [startDate, endDate] } },
                    { status: 1 }
                  ]
                }
              })

              const mb51 = files.find(f => f.type === 'mb51')
              const main = files.find(f => f.type === 'main')

              if (!mb51 || !main) {
                resolve({ success: false, error: 'MB51 atau Main file belum diupload', plant })
                return
              }

              // Buat record baru untuk hasil generate
              const report = await report_inven.create({
                name: 'output_report_inventory',
                path: '',
                type: 'output',
                status: 0 // Processing
              })

              console.log(`[${plant}] Starting Python worker...`)
              
              // Debug PATH
              console.log('=== DEBUG PATH ===');
              console.log('PATH:', process.env.PATH);
              console.log('Which python:', require('child_process').execSync('which python || echo "not found"').toString().trim());
              console.log('==================');

              
              // Spawn python worker
              const py = spawn(pythonPathProd, [
                path.join(__dirname, '../workers/generate_inventory_report.py')
              ])

              const payload = {
                report_id: report.id,
                files: {
                  mb51: mb51.path,
                  main: main.path
                },
                master_inventory: masterInventory.map(m => m.toJSON()),
                master_movement: masterMovement.map(m => m.toJSON())
              }

              console.log(`[${plant}] Sending payload to Python`)

              py.stdin.write(JSON.stringify(payload))
              py.stdin.end()

              let stdoutData = ''
              let stderrData = ''
              
              py.stdout.on('data', data => { 
                stdoutData += data.toString()
                console.log(`[${plant}] Python stdout:`, data.toString())
              })
              
              py.stderr.on('data', data => { 
                stderrData += data.toString()
                console.log(`[${plant}] Python stderr:`, data.toString())
              })

              py.on('error', async (error) => {
                console.error(`[${plant}] Failed to start Python process:`, error)
                await report.destroy()
                resolve({ success: false, error: 'Failed to start Python process: ' + error.message, plant })
              })

              py.on('close', async (code) => {
                console.log(`[${plant}] Python process exited with code ${code}`)
                
                try {
                  if (code === 0) {
                    // Parse JSON from stdout
                    const jsonMatch = stdoutData.match(/\{[\s\S]*\}/)
                    if (!jsonMatch) {
                      throw new Error('No JSON found in Python output')
                    }
                    
                    const result = JSON.parse(jsonMatch[0])
                    
                    if (!result.success) {
                      throw new Error(result.error || 'Unknown Python error')
                    }
                    
                    console.log(`[${plant}] Report generated successfully:`, result.output_path)
                    
                    // Update database
                    await report.update({ 
                      path: result.output_path, 
                      status: 2, // Success
                      name: `${plant}_output_report_inventory_${result.timestamp}`,
                      date_report: startDate,
                      plant: plant,
                      user_upload: username
                    })
                    
                    resolve({ 
                      success: true, 
                      plant,
                      output_path: result.output_path,
                      rows_written: result.rows_written,
                      report_month: result.report_month
                    })
                  } else {
                    // Python exited with error
                    let errorMsg = 'Python process failed'
                    
                    try {
                      const jsonMatch = stdoutData.match(/\{[\s\S]*\}/)
                      if (jsonMatch) {
                        const errorResult = JSON.parse(jsonMatch[0])
                        errorMsg = errorResult.error || errorMsg
                        console.error(`[${plant}] Python error details:`, errorResult.trace)
                      }
                    } catch (e) {
                      console.error(`[${plant}] Could not parse Python error output`)
                    }
                    
                    await report.destroy()
                    resolve({ success: false, error: errorMsg, plant })
                  }
                } catch (parseError) {
                  console.error(`[${plant}] Error parsing Python output:`, parseError)
                  await report.destroy()
                  resolve({ success: false, error: 'Failed to parse Python output: ' + parseError.message, plant })
                }
              })

              // Set timeout (10 minutes)
              const timeoutId = setTimeout(() => {
                if (!py.killed) {
                  console.log(`[${plant}] Python process timeout, killing...`)
                  py.kill()
                  report.destroy()
                  resolve({ success: false, error: 'Report generation timeout', plant })
                }
              }, 600000)

              // Clear timeout saat process selesai
              py.on('exit', () => {
                clearTimeout(timeoutId)
              })

            } catch (err) {
              console.error(`[${plant}] Error:`, err)
              reject(err)
            }
          })
        }

        // 🔧 PERBAIKAN: Jalankan semua plant SECARA BERURUTAN dengan for...of
        console.log(`Starting processing for ${listPlant.length} plants...`)
        
        for (const plant of listPlant) {
          console.log(`\n=== Processing plant: ${plant} ===`)
          const result = await processPlant(plant)
          
          if (result.success) {
            cekGenerate.push(result)
            console.log(`✓ ${plant} SUCCESS`)
          } else {
            cekFalse.push({ text: result.error, plant: result.plant })
            console.log(`✗ ${plant} FAILED: ${result.error}`)
          }
        }

        console.log(`\n=== ALL PLANTS PROCESSED ===`)
        console.log(`Success: ${cekGenerate.length}`)
        console.log(`Failed: ${cekFalse.length}`)

        // Response berdasarkan hasil
        if (cekGenerate.length > 0 && cekFalse.length === 0) {
          return response(res, 'Semua report berhasil di-generate', { 
            success: cekGenerate.length,
            results: cekGenerate 
          }, 200, true)
        } else if (cekGenerate.length > 0 && cekFalse.length > 0) {
          return response(res, 'Sebagian report berhasil di-generate', { 
            success: cekGenerate.length,
            failed: cekFalse.length,
            successList: cekGenerate,
            failedList: cekFalse
          }, 207, true) // 207 Multi-Status
        } else {
          return response(res, 'Semua report gagal di-generate', { 
            failed: cekFalse.length,
            failedList: cekFalse 
          }, 400, false)
        }
      } else {
        return response(res, 'Parameter yang dikirim tidak lengkap', {}, 400, false)
      }
    } catch (err) {
      console.error('Controller error:', err)
      return response(res, err.message, {}, 500, false)
    }
  },
  mergeInventoryReports: async (req, res) => {
    try {

      const username = req.user.name
      const { listIds, date } = req.body // Array of report IDs to merge
      const startDate = moment(date).startOf('month').toDate();
      
      if (!listIds || !Array.isArray(listIds) || listIds.length === 0) {
        return response(res, 'listIds harus berupa array dan tidak boleh kosong', {}, 400, false)
      }
      
      if (listIds.length === 1) {
        return response(res, 'Minimal 2 report untuk di-merge', {}, 400, false)
      }
      
      console.log(`Merging ${listIds.length} reports:`, listIds)
      
      // Fetch all reports from database
      const reports = await report_inven.findAll({
        where: {
          id: listIds,
          type: 'output', // Only merge output reports
          status: 2 // Only merge successful reports
        }
      })
      
      if (reports.length === 0) {
        return response(res, 'Tidak ada report yang valid untuk di-merge', {}, 400, false)
      }
      
      if (reports.length !== listIds.length) {
        return response(res, 
          `Hanya ${reports.length} dari ${listIds.length} report yang valid (status=success, type=output)`, 
          {}, 400, false)
      }
      
      // Get file paths
      const filePaths = reports.map(r => r.path)
      
      // Validate all files exist
      const fs = require('fs')
      for (const filePath of filePaths) {
        if (!fs.existsSync(filePath)) {
          return response(res, `File tidak ditemukan: ${filePath}`, {}, 404, false)
        }
      }
      
      // Create new record for merged report
      const mergedReport = await report_inven.create({
        name: 'consolidated_report_inventory',
        path: '',
        type: 'consolidated',
        status: 0 // Processing
      })
      
      console.log('Starting Python merge worker...')
      
      // Spawn python worker
      const { spawn } = require('child_process')
      const path = require('path')
      
      const py = spawn(pythonPathProd, [
        path.join(__dirname, '../workers/merge_inventory_reports.py')
      ])
      
      const payload = {
        report_id: mergedReport.id,
        file_paths: filePaths
      }
      
      console.log('Sending payload to Python:', JSON.stringify(payload, null, 2))
      
      py.stdin.write(JSON.stringify(payload))
      py.stdin.end()
      
      let stdoutData = ''
      let stderrData = ''
      let responseSent = false
      
      py.stdout.on('data', data => {
        stdoutData += data.toString()
        console.log('Python stdout:', data.toString())
      })
      
      py.stderr.on('data', data => {
        stderrData += data.toString()
        console.log('Python stderr:', data.toString())
      })
      
      py.on('error', async (error) => {
        console.error('Failed to start Python process:', error)
        if (!responseSent) {
          responseSent = true
          await mergedReport.destroy() // Failed
          return response(res, 'Failed to start Python process: ' + error.message, {}, 500, false)
        }
      })
      
      py.on('close', async (code) => {
        console.log(`Python process exited with code ${code}`)
        console.log('Final stdout:', stdoutData)
        console.log('Final stderr:', stderrData)
        
        if (responseSent) return
        responseSent = true
        
        try {
          if (code === 0) {
            // Parse JSON from stdout
            const jsonMatch = stdoutData.match(/\{[\s\S]*\}/)
            if (!jsonMatch) {
              throw new Error('No JSON found in Python output')
            }
            
            const result = JSON.parse(jsonMatch[0])
            
            if (!result.success) {
              throw new Error(result.error || 'Unknown Python error')
            }
            
            console.log('Reports merged successfully:', result.output_path)
            
            // Update database
            let fullplant = ''
            reports.map(x => fullplant = `${fullplant === '' ? '' : fullplant + ', '}${x.plant}`)

            await mergedReport.update({
              path: result.output_path,
              status: 3, // Success
              name: `consolidated_report_${result.timestamp}`,
              user_upload: username,
              date_report: startDate,
              info: `Merge report ${fullplant}`
            })
            
            return response(res, 'Reports merged successfully', {
              link: result.output_path,
              report_id: mergedReport.id,
              total_files_merged: result.total_files_merged,
              total_data_rows: result.total_data_rows,
              file_size: result.file_size
            })
          } else {
            // Python exited with error
            let errorMsg = 'Python process failed'
            
            try {
              const jsonMatch = stdoutData.match(/\{[\s\S]*\}/)
              if (jsonMatch) {
                const errorResult = JSON.parse(jsonMatch[0])
                errorMsg = errorResult.error || errorMsg
                console.error('Python error details:', errorResult.trace)
              }
            } catch (e) {
              console.error('Could not parse Python error output')
            }
            
            await mergedReport.destroy() // Failed
            return response(res, 'Failed to merge reports: ' + errorMsg, {
              stderr: stderrData,
              exit_code: code
            }, 500, false)
          }
        } catch (parseError) {
          console.error('Error parsing Python output:', parseError)
          console.error('Raw stdout:', stdoutData)
          console.error('Raw stderr:', stderrData)
          
          await mergedReport.destroy() // Failed
          return response(res, 'Failed to parse Python output: ' + parseError.message, {
            stdout: stdoutData,
            stderr: stderrData
          }, 500, false)
        }
      })
      
      // Set timeout (5 minutes)
      const timeoutId = setTimeout(() => {
        if (!responseSent && !py.killed) {
          console.log('Python process timeout, killing...')
          responseSent = true
          py.kill()
          mergedReport.destroy()
          return response(res, 'Merge operation timeout', {}, 500, false)
        }
      }, 300000) // 5 minutes
      
      // Clear timeout on exit
      py.on('exit', () => {
        clearTimeout(timeoutId)
      })
      
    } catch (err) {
      console.error('Controller error:', err)
      return response(res, err.message, {}, 500, false)
    }
  },
  generateInventoryReportReadData: async (req, res) => {
    try {
        const { listId } = req.body;
        let dataId = [];
        if (listId && listId.length > 0) {
            dataId = listId.map(function(id) {
                return { id: id };
            });
        }

        const files = await report_inven.findAll({ where: { [Op.or]: dataId } });
        const mb51 = files.find(f => f.type === 'mb51');
        const main = files.find(f => f.type === 'main');

        if (!mb51 || !main) {
            return response(res, 'File MB51 atau main.xlsx belum diupload', {}, 400, false);
        }

        const py = spawn("C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python313\\python.exe", [
            path.join(__dirname, '../workers/read_excel.py')
        ]);

        const payload = {
            files: {
                mb51: mb51.path,
                main: main.path
            }
        };

        py.stdin.write(JSON.stringify(payload));
        py.stdin.end();

        let outputChunks = [];
        py.stdout.on('data', chunk => outputChunks.push(Buffer.from(chunk)));
        py.stderr.on('data', data => console.error('Python error:', data.toString()));

        py.on('close', async code => {
            if (code === 0) {
                try {
                    const outputStr = Buffer.concat(outputChunks).toString('utf8');
                    const result = JSON.parse(outputStr);
                    return response(res, 'Report generated successfully', { data: result.mb51 });
                } catch(err) {
                    console.error('JSON parse error:', err);
                    return response(res, 'Failed to parse JSON from Python', {}, 500, false);
                }
            } else {
                return response(res, 'Failed to generate report', {}, 500, false);
            }
        });
    } catch (err) {
        console.error(err);
        return response(res, err.message, {}, 500, false);
    }
  }
}
