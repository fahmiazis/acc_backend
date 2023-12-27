const { datamerge, logupload } = require('../models')
// const joi = require('joi')
const { Op } = require('sequelize')
const response = require('../helpers/response')
const fs = require('fs')
const { pagination } = require('../helpers/pagination')
const uploadMaster = require('../helpers/uploadMaster')
// const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const moment = require('moment')
const excel = require('exceljs')
const xlsx = require('xlsx')
const vs = require('fs-extra')
// const JSZip = require('JSZip')
const { APP_URL } = process.env

module.exports = {
  uploadMasterDataMerge: async (req, res) => {
    // const level = req.user.level
    // if (level === 1) {
    req.setTimeout(14400000)
    uploadMaster(req, res, async function (err) {
      try {
        if (err instanceof multer.MulterError) {
          if (err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length === 0) {
            console.log(err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length > 0)
            return response(res, 'fieldname doesnt match', {}, 500, false)
          }
          console.log('masuk if')
          return response(res, err.message, {}, 500, false)
        } else if (err) {
          console.log('masuk else if')
          return response(res, err.message, {}, 401, false)
        }
        const arr = []
        const failarr = []
        for (let f = 0; f < req.files.length; f++) {
          const dokumen = `assets/masters/${req.files[f].filename}`
          // const rows = await readXlsxFile(dokumen)
          console.log(req.files[f])
          if (req.files[f].size === 0) {
            const newName = `${moment().format('DDMMYYYYhmms')}~${req.files[f].originalname}`
            const newPath = `assets/merge/${newName}`
            failarr.push(req.files[f].originalname)
            fs.rename(dokumen, newPath, async function (err) {
              if (err) {
                console.log(err)
                console.log('failed upload, remove & delete ')
                const data = {
                  name: newName,
                  error: 'size dokumen 0 kb',
                  status: 'gagal'
                }
                await logupload.create(data)
              } else {
                console.log('success upload, remove & delete')
                const data = {
                  name: newName,
                  error: 'size dokumen 0 kb',
                  status: 'gagal'
                }
                await logupload.create(data)
              }
            })
          } else {
            const rows = xlsx.readFile(dokumen)
            // const count = []
            const cek = [
              'KODE_OUTLET',
              'NAMA_OUTLET',
              'KODE_SALES',
              'NAMA_SALES',
              'TGL_FAKTUR',
              'NO_FAKTUR',
              'GROSS_SALES',
              'RP_DISCPC',
              'DISC1',
              'DISC2',
              'PRO_AMOUNT',
              'CASH_DISCT',
              'PPN',
              'TOTAL',
              'TYPE',
              'PCODE',
              'NAMA_PRODUK',
              'QTY_PCS',
              'KODE_RETUR',
              'NAMA_RETUR',
              'TGL_RETUR',
              'INVORT',
              'REMARK',
              'KETERANGAN'
            ]
            const headRow = Object.keys(rows.Sheets[Object.keys(rows.Sheets)[0]])
            const dataRow = rows.Sheets[Object.keys(rows.Sheets)[0]]
            let parRow = ''
            for (let i = 0; i < headRow.length; i++) {
              const cekLength = headRow[headRow.length - i]
              if (cekLength !== undefined) {
                if (isNaN(parseInt(cekLength.substring(1))) === false) {
                  parRow = cekLength
                  break
                }
              }
            }
            const splRow = parRow
            const lengthRow = splRow.substring(1)
            // console.log(splRow)
            console.log(lengthRow)
            if (cek.length > 0) {
              const result = []
              if (result.length > 0) {
                return response(res, 'there is duplication in your file master', { result }, 404, false)
              } else {
                const sendArr = []
                if (isNaN(parseInt(lengthRow)) === false) {
                  let startRow = ''
                  for (let i = 0; i < parseInt(lengthRow); i++) {
                    const cekLength = dataRow[`A${i}`] !== undefined ? dataRow[`A${i}`].w.replace(/[()-._,?/{}]/g, ' ') : 'null'
                    if (cekLength !== undefined) {
                      const cekName = cekLength.split(' ')[cekLength.split(' ').length - 1]
                      if (cekName.toLowerCase() === 'mt' || cekName.toLowerCase() === 'gt') {
                        startRow = cekLength
                        break
                      }
                    }
                  }
                  console.log(startRow)
                  if (startRow === '') {
                    const newName = `${moment().format('DDMMYYYYhmms')}~${req.files[f].originalname}`
                    const newPath = `assets/merge/${newName}`
                    failarr.push(req.files[f].originalname)
                    fs.rename(dokumen, newPath, async function (err) {
                      if (err) {
                        console.log(err)
                        console.log('failed upload, remove & delete, cant read file')
                        const data = {
                          name: newName,
                          error: 'Nama depo tidak ditemukan atau tidak sesuai format (tidak mengandung gt/mt)',
                          status: 'gagal'
                        }
                        await logupload.create(data)
                      } else {
                        console.log('success upload, remove & delete, cant read file')
                        const data = {
                          name: newName,
                          error: 'Nama depo tidak ditemukan atau tidak sesuai format (tidak mengandung gt/mt)',
                          status: 'gagal'
                        }
                        await logupload.create(data)
                      }
                    })
                  } else {
                    for (let i = 0; i < parseInt(lengthRow); i++) {
                    // for (let i = parseInt(lengthRow) - 1; i < parseInt(lengthRow); i++) {
                      if (dataRow[`A${i}`] !== undefined && isNaN(parseFloat(dataRow[`A${i}`].w)) === false) {
                        const sendData = {
                          nama_depo: startRow,
                          kode_outlet: dataRow[`A${i}`] !== undefined ? dataRow[`A${i}`].w : '',
                          nama_outlet: dataRow[`B${i}`] !== undefined ? dataRow[`B${i}`].w : '',
                          kode_sales: dataRow[`C${i}`] !== undefined ? dataRow[`C${i}`].w : '',
                          nama_sales: dataRow[`D${i}`] !== undefined ? dataRow[`D${i}`].w : '',
                          tgl_faktur: dataRow[`E${i}`] !== undefined ? dataRow[`E${i}`].w : '',
                          no_faktur: dataRow[`F${i}`] !== undefined ? dataRow[`F${i}`].w : '',
                          gross_sales: dataRow[`G${i}`] !== undefined ? dataRow[`G${i}`].w : '',
                          rp_discpc: dataRow[`H${i}`] !== undefined ? dataRow[`H${i}`].w : '',
                          disc1: dataRow[`I${i}`] !== undefined ? dataRow[`I${i}`].w : '',
                          disc2: dataRow[`J${i}`] !== undefined ? dataRow[`J${i}`].w : '',
                          pro_amount: dataRow[`K${i}`] !== undefined ? dataRow[`K${i}`].w : '',
                          cash_disct: dataRow[`L${i}`] !== undefined ? dataRow[`L${i}`].w : '',
                          ppn: dataRow[`M${i}`] !== undefined ? dataRow[`M${i}`].w : '',
                          total: dataRow[`N${i}`] !== undefined ? dataRow[`N${i}`].w : '',
                          type: dataRow[`O${i}`] !== undefined ? dataRow[`O${i}`].w : '',
                          pcode: dataRow[`P${i}`] !== undefined ? dataRow[`P${i}`].w : '',
                          nama_produk: dataRow[`Q${i}`] !== undefined ? dataRow[`Q${i}`].w : '',
                          qty_pcs: dataRow[`R${i}`] !== undefined ? dataRow[`R${i}`].w : '',
                          kode_retur: dataRow[`S${i}`] !== undefined ? dataRow[`S${i}`].w : '',
                          nama_retur: dataRow[`T${i}`] !== undefined ? dataRow[`T${i}`].w : '',
                          tgl_retur: dataRow[`U${i}`] !== undefined ? dataRow[`U${i}`].w : '',
                          invort: dataRow[`V${i}`] !== undefined ? dataRow[`V${i}`].w : '',
                          remark: dataRow[`W${i}`] !== undefined ? dataRow[`W${i}`].w : '',
                          keterangan: dataRow[`X${i}`] !== undefined ? dataRow[`X${i}`].w : ''
                        }
                        // console.log(sendData)
                        // const createDataMerge = await datamerge.create(sendData)
                        // if (createDataMerge) {
                        //   arr.push(1)
                        // }
                        sendArr.push(sendData)
                      }
                    // else {
                    //   console.log(dataRow[`A${i}`])
                    //   console.log(isNaN(parseFloat(dataRow[`A${i}`])))
                    // }
                    }
                    if (sendArr.length > 0) {
                      const createDataMerge = await datamerge.bulkCreate(sendArr)
                      if (createDataMerge) {
                        arr.push(req.files[f].originalname)
                        fs.unlink(dokumen, async function (err) {
                          if (err) {
                            console.log('successfully upload file master')
                          } else {
                            console.log('successfully upload file master')
                          }
                          const data = {
                            name: req.files[f].originalname,
                            status: 'success'
                          }
                          await logupload.create(data)
                        })
                      } else {
                        failarr.push(req.files[f].originalname)
                        fs.unlink(dokumen, async function (err) {
                          if (err) {
                            console.log('successfully upload file master else')
                          } else {
                            console.log('successfully upload file master else')
                          }
                          const data = {
                            name: req.files[f].originalname,
                            status: 'success'
                          }
                          await logupload.create(data)
                        })
                      }
                    } else {
                      failarr.push(req.files[f].originalname)
                      fs.unlink(dokumen, async function (err) {
                        if (err) {
                          console.log('failed upload file master')
                        } else {
                          console.log('failed upload file master')
                        }
                        const data = {
                          name: req.files[f].originalname,
                          error: 'Dokumen tidak terbaca oleh sistem (corrupt file)',
                          status: 'gagal'
                        }
                        await logupload.create(data)
                      })
                    }
                  }
                } else {
                  const newName = `${moment().format('DDMMYYYYhmms')}~${req.files[f].originalname}`
                  const newPath = `assets/merge/${newName}`
                  failarr.push(req.files[f].originalname)
                  fs.rename(dokumen, newPath, async function (err) {
                    if (err) {
                      console.log(err)
                      console.log('failed upload, remove & delete, cant read file')
                      const data = {
                        name: newName,
                        error: 'Dokumen tidak terbaca oleh sistem (corrupt file)',
                        status: 'gagal'
                      }
                      await logupload.create(data)
                    } else {
                      console.log('success upload, remove & delete, cant read file')
                      const data = {
                        name: newName,
                        error: 'Dokumen tidak terbaca oleh sistem (corrupt file)',
                        status: 'gagal'
                      }
                      await logupload.create(data)
                    }
                  })
                }
              }
            } else {
              fs.unlink(dokumen, function (err) {
                if (err) {
                  console.log('success delete')
                } else {
                  console.log('success delete')
                }
              })
            }
          }
        }
        if (arr.length > 0) {
          return response(res, 'Succes upload', { succesUpload: arr, failedUpload: failarr })
        } else {
          console.log(req.files)
          return response(res, 'failed upload', { failedUpload: failarr })
        }
      } catch (error) {
        console.log('masuk catch')
        return response(res, error.message, {}, 500, false)
      }
    })
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  },
  uploadRar: async (req, res) => {
    // const level = req.user.level // eslint-disable-line
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
        fs.unlink(dokumen, function (err) {
          if (err) {
            return response(res, 'success read failed upload delete', { })
          } else {
            return response(res, 'success read and delete failed upload', { })
          }
        })
        // const dokumen = 'https://cloud.pinusmerahabadi.co.id/index.php/s/QgNDGy8Pby2LKwY/download?path=%2F&files=2019-01.zip'
        // const dataTemp = []
        // const arr = []
        // fs.readFile(dokumen, function (err, data) {
        //   if (!err) {
        //     const zip = new JSZip()
        //     zip.loadAsync(data).then(async function (contents) {
        //       const xlTemp = []
        //       // xlTemp.push(rows[29][0])
        //       const dataFile = contents.files
        //       const convData = Object.keys(dataFile)
        //       for (let i = 1; i < convData.length; i++) {
        //         await zip.file(convData[i]).async('nodebuffer').then(async function (content) {
        //           // const rows = await readXlsxFile(Buffer.from(content))
        //           // console.log(rows)
        //           const rows = xlsx.read(content)
        //           const headRow = Object.keys(rows.Sheets[Object.keys(rows.Sheets)[0]])
        //           const dataRow = rows.Sheets[Object.keys(rows.Sheets)[0]]
        //           const parRow = headRow[headRow.length - 2]
        //           const initialValue = ''
        //           const splRow = parRow.split('')
        //           splRow.shift()
        //           const lengthRow = splRow.reduce(
        //             (accumulator, currentValue) => accumulator + currentValue,
        //             initialValue
        //           )
        //           console.log(headRow)
        //           console.log(splRow)
        //           console.log(lengthRow)
        //           // for (let i = 0; i < 33; i++) {
        //           for (let i = 0; i < parseInt(initialValue); i++) {
        //             if (dataRow[`A${i}`] !== undefined && isNaN(parseFloat(dataRow[`A${i}`])) === false) {
        //               const sendData = {
        //                 nama_depo: dataRow.A10 !== undefined ? dataRow.A10.w : '',
        //                 kode_outlet: dataRow[`A${i}`] !== undefined ? dataRow[`A${i}`].w : '',
        //                 nama_outlet: dataRow[`B${i}`] !== undefined ? dataRow[`B${i}`].w : '',
        //                 kode_sales: dataRow[`C${i}`] !== undefined ? dataRow[`C${i}`].w : '',
        //                 nama_sales: dataRow[`D${i}`] !== undefined ? dataRow[`D${i}`].w : '',
        //                 tgl_faktur: dataRow[`E${i}`] !== undefined ? dataRow[`E${i}`].w : '',
        //                 no_faktur: dataRow[`F${i}`] !== undefined ? dataRow[`F${i}`].w : '',
        //                 gross_sales: dataRow[`G${i}`] !== undefined ? dataRow[`G${i}`].w : '',
        //                 rp_discpc: dataRow[`H${i}`] !== undefined ? dataRow[`H${i}`].w : '',
        //                 disc1: dataRow[`I${i}`] !== undefined ? dataRow[`I${i}`].w : '',
        //                 disc2: dataRow[`J${i}`] !== undefined ? dataRow[`J${i}`].w : '',
        //                 pro_amount: dataRow[`K${i}`] !== undefined ? dataRow[`K${i}`].w : '',
        //                 cash_disct: dataRow[`L${i}`] !== undefined ? dataRow[`L${i}`].w : '',
        //                 ppn: dataRow[`M${i}`] !== undefined ? dataRow[`M${i}`].w : '',
        //                 total: dataRow[`N${i}`] !== undefined ? dataRow[`N${i}`].w : '',
        //                 type: dataRow[`O${i}`] !== undefined ? dataRow[`O${i}`].w : '',
        //                 pcode: dataRow[`P${i}`] !== undefined ? dataRow[`P${i}`].w : '',
        //                 nama_produk: dataRow[`Q${i}`] !== undefined ? dataRow[`Q${i}`].w : '',
        //                 qty_pcs: dataRow[`R${i}`] !== undefined ? dataRow[`R${i}`].w : '',
        //                 kode_retur: dataRow[`S${i}`] !== undefined ? dataRow[`S${i}`].w : '',
        //                 nama_retur: dataRow[`T${i}`] !== undefined ? dataRow[`T${i}`].w : '',
        //                 tgl_retur: dataRow[`U${i}`] !== undefined ? dataRow[`U${i}`].w : '',
        //                 invort: dataRow[`V${i}`] !== undefined ? dataRow[`V${i}`].w : '',
        //                 remark: dataRow[`W${i}`] !== undefined ? dataRow[`W${i}`].w : '',
        //                 keterangan: dataRow[`X${i}`] !== undefined ? dataRow[`X${i}`].w : ''
        //               }
        //               await datamerge.create(sendData)
        //             }
        //           }
        //           // console.log(parRow)
        //           // console.log(lengthRow)
        //           // console.log(dataRow[parRow])
        //           // console.log(dataRow.A29)
        //         })
        //         xlTemp.push(convData[i])
        //       }
        //       if (xlTemp.length > 0) {
        //         fs.unlink(dokumen, function (err) {
        //           if (err) {
        //             return response(res, 'success upload read failed delete', { xlTemp })
        //           } else {
        //             return response(res, 'success upload read and delete', { xlTemp })
        //           }
        //         })
        //       } else {
        //         fs.unlink(dokumen, function (err) {
        //           if (err) {
        //             return response(res, 'success read failed upload delete', { xlTemp })
        //           } else {
        //             return response(res, 'success read and delete failed upload', { xlTemp })
        //           }
        //         })
        //       }
        //     })
        //   }
        // })
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  },
  getDataMerge: async (req, res) => {
    try {
      // const kode = req.user.kode
      const findDataMerge = await datamerge.findAll()
      if (findDataMerge.length > 0) {
        return response(res, 'succes get datamerge', { result: findDataMerge, length: findDataMerge.length })
      } else {
        return response(res, 'failed get datamerge', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getAllDataMerge: async (req, res) => {
    try {
      let { limit, page, search, sort, typeSort } = req.query
      let searchValue = ''
      let sortValue = ''
      let typeSortValue = ''
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
      } else {
        limit = parseInt(limit)
      }

      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }

      if (typeof typeSort === 'object') {
        typeSortValue = Object.values(typeSort)[0]
      } else {
        typeSortValue = typeSort || 'ASC'
      }
      const findDataMerge = await datamerge.findAndCountAll({
        where: {
          [Op.or]: [
            { kode_depo: { [Op.like]: `%${searchValue}%` } },
            { nama_depo: { [Op.like]: `%${searchValue}%` } },
            { kode_outlet: { [Op.like]: `%${searchValue}%` } },
            { nama_outlet: { [Op.like]: `%${searchValue}%` } },
            { kode_sales: { [Op.like]: `%${searchValue}%` } },
            { nama_sales: { [Op.like]: `%${searchValue}%` } },
            { tgl_faktur: { [Op.like]: `%${searchValue}%` } },
            { no_faktur: { [Op.like]: `%${searchValue}%` } },
            { gross_sales: { [Op.like]: `%${searchValue}%` } },
            { rp_discpc: { [Op.like]: `%${searchValue}%` } },
            { disc1: { [Op.like]: `%${searchValue}%` } },
            { disc2: { [Op.like]: `%${searchValue}%` } },
            { pro_amount: { [Op.like]: `%${searchValue}%` } },
            { cash_disct: { [Op.like]: `%${searchValue}%` } },
            { ppn: { [Op.like]: `%${searchValue}%` } },
            { total: { [Op.like]: `%${searchValue}%` } },
            { type: { [Op.like]: `%${searchValue}%` } },
            { pcode: { [Op.like]: `%${searchValue}%` } },
            { nama_produk: { [Op.like]: `%${searchValue}%` } },
            { qty_pcs: { [Op.like]: `%${searchValue}%` } },
            { kode_retur: { [Op.like]: `%${searchValue}%` } },
            { nama_retur: { [Op.like]: `%${searchValue}%` } },
            { tgl_retur: { [Op.like]: `%${searchValue}%` } },
            { invort: { [Op.like]: `%${searchValue}%` } },
            { remark: { [Op.like]: `%${searchValue}%` } },
            { keterangan: { [Op.like]: `%${searchValue}%` } }
          ]
        },
        order: [[sortValue, typeSortValue]],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/datamerge/get', req.query, page, limit, findDataMerge.count)
      if (findDataMerge) {
        return response(res, 'succes get datamerge', { result: findDataMerge, pageInfo })
      } else {
        return response(res, 'failed get datamerge', { result: [], pageInfo })
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getAllLog: async (req, res) => {
    try {
      let { limit, page, search, sort, typeSort } = req.query
      let searchValue = ''
      let sortValue = ''
      let typeSortValue = ''
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
      } else {
        limit = parseInt(limit)
      }

      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }

      if (typeof typeSort === 'object') {
        typeSortValue = Object.values(typeSort)[0]
      } else {
        typeSortValue = typeSort || 'ASC'
      }
      const findDataLog = await logupload.findAndCountAll({
        where: {
          [Op.or]: [
            { name: { [Op.like]: `%${searchValue}%` } },
            { error: { [Op.like]: `%${searchValue}%` } },
            { status: { [Op.like]: `%${searchValue}%` } }
          ]
        },
        order: [[sortValue, typeSortValue]],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/datamerge/log', req.query, page, limit, findDataLog.count)
      if (findDataLog) {
        return response(res, 'succes get log upload', { result: findDataLog, pageInfo })
      } else {
        return response(res, 'failed get log upload', { result: [], pageInfo })
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailDataMerge: async (req, res) => {
    try {
      const id = req.params.id
      const findDataMerge = await datamerge.findByPk(id)
      if (findDataMerge) {
        return response(res, 'succes get detail datamerge', { result: findDataMerge })
      } else {
        return response(res, 'failed get datamerge', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deleteDataMerge: async (req, res) => {
    try {
      const id = req.params.id
      const findDataMerge = await datamerge.findByPk(id)
      if (findDataMerge) {
        const delDataMerge = await findDataMerge.destroy()
        if (delDataMerge) {
          return response(res, 'succes delete datamerge', { result: findDataMerge })
        } else {
          return response(res, 'failed destroy datamerge', {}, 404, false)
        }
      } else {
        return response(res, 'failed get datamerge', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  exportSqlDataMerge: async (req, res) => {
    try {
      const result = await datamerge.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        const arr = []
        const header = [
          'KODE_OUTLET',
          'NAMA_OUTLET',
          'KODE_SALES',
          'NAMA_SALES',
          'TGL_FAKTUR',
          'NO_FAKTUR',
          'GROSS_SALES',
          'RP_DISCPC',
          'DISC1',
          'DISC2',
          'PRO_AMOUNT',
          'CASH_DISCT',
          'PPN',
          'TOTAL',
          'TYPE',
          'PCODE',
          'NAMA_PRODUK',
          'QTY_PCS',
          'KODE_RETUR',
          'NAMA_RETUR',
          'TGL_RETUR',
          'INVORT',
          'REMARK',
          'KETERANGAN'
        ]
        const key = [
          'kode_outlet',
          'nama_outlet',
          'kode_sales',
          'nama_sales',
          'tgl_faktur',
          'no_faktur',
          'gross_sales',
          'rp_discpc',
          'disc1',
          'disc2',
          'pro_amount',
          'cash_disct',
          'ppn',
          'total',
          'type',
          'pcode',
          'nama_produk',
          'qty_pcs',
          'kode_retur',
          'nama_retur',
          'tgl_retur',
          'invort',
          'remark',
          'keterangan'
        ]
        for (let i = 0; i < header.length; i++) {
          let temp = { header: header[i], key: key[i] }
          arr.push(temp)
          temp = {}
        }
        worksheet.columns = arr
        const cek = worksheet.addRows(result)
        if (cek) {
          const name = new Date().getTime().toString().concat('-datamerge').concat('.xlsx')
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
      const findDataMerge = await datamerge.findAll()
      if (findDataMerge) {
        const temp = []
        for (let i = 0; i < findDataMerge.length; i++) {
          const findDel = await datamerge.findByPk(findDataMerge[i].id)
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
