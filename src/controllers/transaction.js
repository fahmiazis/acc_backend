const { pagination } = require('../helpers/pagination')
const { documents, sequelize, Path, depo, activity, pic, email } = require('../models')
const { Op, QueryTypes } = require('sequelize')
const response = require('../helpers/response')
const joi = require('joi')
const uploadHelper = require('../helpers/upload')
const multer = require('multer')
const fs = require('fs')
// const vs = require('fs-extra')
// const { APP_URL } = process.env
// const excel = require('exceljs')
const mailer = require('../helpers/mailer')
const moment = require('moment')
// const xlsx = require('xlsx')

module.exports = {
  dashboard: async (req, res) => {
    try {
      let { limit, page, search, sort, typeSort, time, tipe } = req.query
      let searchValue = ''
      let sortValue = ''
      let typeSortValue = ''
      let timeValue = ''
      let tipeValue = ''
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
      if (typeof time === 'object') {
        timeValue = Object.values(time)[0]
      } else {
        timeValue = time || ''
      }
      if (typeof tipe === 'object') {
        tipeValue = Object.values(tipe)[0]
      } else {
        tipeValue = tipe || 'daily'
      }
      if (typeof typeSort === 'object') {
        typeSortValue = Object.values(typeSort)[0]
      } else {
        typeSortValue = typeSort || 'DESC'
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
      //   const id = req.user.id
      const level = req.user.level
      const kode = req.user.kode
      const timeUser = moment().utc().format('YYYY-MM-DD')
      const now = timeValue === '' ? new Date(moment().utc().format('YYYY-MM-DD')) : new Date(moment(timeValue).utc().format('YYYY-MM-DD'))
      const tomo = timeValue === '' ? new Date(moment().utc().format('YYYY-MM-DD 24:00:00')) : new Date(moment(timeValue).utc().format('YYYY-MM-DD 24:00:00'))
      if (level === 4) {
        const result = await depo.findOne({
          where: {
            kode_plant: kode
          }
        })
        if (result) {
          const cabang = result.status_depo
          const results = await documents.findAndCountAll({
            where: {
              [Op.or]: [
                { nama_dokumen: { [Op.like]: `%${searchValue}%` } },
                { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
              ],
              [Op.and]: [
                { status_depo: cabang },
                { uploadedBy: 'sa' }
              ],
              [Op.not]: { status: 'inactive' }
            },
            order: [[sortValue, typeSortValue]],
            limit: limit,
            offset: (page - 1) * limit
          })
          const pageInfo = pagination('/dokumen/get', req.query, page, limit, results.count)
          if (results) {
            const cek = await sequelize.query(`SELECT kode_plant, tipe from activities WHERE (kode_plant='${kode}' AND tipe='sa') AND jenis_dokumen LIKE '%${tipeValue}%'  AND createdAt LIKE '%${timeUser}%' LIMIT 1`, {
              type: QueryTypes.SELECT
            })
            if (cek.length > 0) {
              return response(res, 'list dokumen', { results, pageInfo })
            } else {
              const data = {
                kode_plant: kode,
                status: 'Belum Upload',
                documentDate: new Date(moment().subtract(1, 'days')),
                access: 'unlock',
                jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                tipe: 'sa'
              }
              const create = await activity.create(data)
              if (create) {
                return response(res, 'list dokumen', { results, pageInfo })
              } else {
                return response(res, 'failed to get dokumen', {}, 404, false)
              }
            }
          } else {
            return response(res, 'failed to get user', {}, 404, false)
          }
        } else {
          return response(res, 'user tidak terhubung dengan depo manapun', {}, 404, false)
        }
      } else if (level === 5) {
        const result = await depo.findOne({
          where: {
            kode_plant: kode
          }
        })
        if (result) {
          const cabang = result.status_depo
          const results = await documents.findAndCountAll({
            where: {
              [Op.or]: [
                { nama_dokumen: { [Op.like]: `%${searchValue}%` } },
                { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
              ],
              [Op.and]: [
                { status_depo: cabang },
                { uploadedBy: 'kasir' }
              ],
              [Op.not]: { status: 'inactive' }
            },
            order: [[sortValue, typeSortValue]]
          })
          const pageInfo = pagination('/dokumen/get', req.query, page, limit, results.count)
          if (results) {
            const cek = await sequelize.query(`SELECT kode_plant, tipe from activities WHERE (kode_plant='${kode}' AND tipe='kasir') AND jenis_dokumen LIKE '%${tipeValue}%' AND createdAt LIKE '%${timeUser}%' LIMIT 1`, {
              type: QueryTypes.SELECT
            })
            if (cek.length > 0) {
              return response(res, 'list dokumen', { results, pageInfo })
            } else {
              const data = {
                kode_plant: kode,
                status: 'Belum Upload',
                documentDate: new Date(moment().subtract(1, 'days')),
                access: 'unlock',
                jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                tipe: 'kasir'
              }
              const create = await activity.create(data)
              if (create) {
                return response(res, 'list dokumen', { results, pageInfo })
              } else {
                return response(res, 'failed to get dokumen', {}, 404, false)
              }
            }
          } else {
            return response(res, 'failed to get dokumen', {}, 404, false)
          }
        } else {
          return response(res, 'user tidak terhubung dengan depo manapun', {}, 404, false)
        }
      } else if (level === 3) {
        const name = req.user.name
        const results = await pic.findAndCountAll({
          where: {
            pic: name
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo'
            }
          ]
        })
        const pageInfo = pagination('/dashboard/get', req.query, page, limit, results.count)
        if (results) {
          const depos = []
          results.rows.map(x => {
            return (
              depos.push(x.depo)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_plant
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_plant },
                        { tipe: 'sa' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'sa' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                sa.push(result.rows[0])
              }
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_plant
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_plant },
                        { tipe: 'kasir' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'kasir' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                kasir.push(result.rows[0])
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            } else {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get dokumen', {}, 404, false)
        }
      } else if (level === 2) {
        const name = req.user.name
        const results = await pic.findAndCountAll({
          where: {
            spv: name
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo'
            }
          ]
        })
        const pageInfo = pagination('/dashboard/get', req.query, page, limit, results.count)
        if (results) {
          const depos = []
          results.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'sa' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'sa' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                sa.push(result.rows[0])
              }
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'kasir' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'kasir' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                kasir.push(result.rows[0])
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            } else {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get dokumen', {}, 404, false)
        }
      } else if (level === 1) {
        const results = await pic.findAndCountAll({
          where: {
            spv: { [Op.like]: '%%' }
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo'
            }
          ]
        })
        const pageInfo = pagination('/dashboard/get', req.query, page, limit, results.count)
        if (results) {
          const depos = []
          results.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'sa' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'sa' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                sa.push(result.rows[0])
              }
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'kasir' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'kasir' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                kasir.push(result.rows[0])
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            } else {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get dokumen', {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadDocument: async (req, res) => {
    const id = req.params.id
    const level = req.user.level
    const kode = req.user.kode
    let time = req.params.time
    if (time !== undefined) {
      time = req.params.time
    } else {
      time = moment().utc().format('YYYY-MM-DD')
    }
    uploadHelper(req, res, async function (err) {
      try {
        if (err instanceof multer.MulterError) {
          if (err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length === 0) {
            console.log(err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length > 0)
            return response(res, 'fieldname doesnt match', {}, 500, false)
          }
          return response(res, err.message, {}, 500, false)
        } else if (err) {
          return response(res, err.message, {}, 401, false)
        } else {
          const dokumen = `assets/documents/${req.file.filename}`
          console.log(req.file)
          if (level === 5) {
            const result = await documents.findByPk(id)
            if (result) {
              const cek = await sequelize.query(`SELECT id from activities WHERE (kode_plant='${kode}' AND tipe='kasir') AND createdAt LIKE '%${time}%' AND jenis_dokumen='${result.jenis_dokumen}' LIMIT 1`, {
                type: QueryTypes.SELECT
              })
              console.log(cek)
              if (cek.length > 0) {
                const send = { dokumen: result.nama_dokumen, activityId: cek[0].id, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                const upload = await Path.create(send)
                return response(res, 'successfully upload dokumen', { upload })
              } else {
                return response(res, 'failed to upload dokumen', {}, 404, false)
              }
            } else {
              return response(res, 'failed to upload dokumen', {}, 404, false)
            }
          } else if (level === 4) {
            const result = await documents.findByPk(id)
            if (result) {
              const cek = await sequelize.query(`SELECT id from activities WHERE (kode_plant='${kode}' AND tipe='sa') AND createdAt LIKE '%${time}%' AND jenis_dokumen='${result.jenis_dokumen}' LIMIT 1`, {
                type: QueryTypes.SELECT
              })
              if (cek.length > 0) {
                const send = { dokumen: result.nama_dokumen, activityId: cek[0].id, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                const upload = await Path.create(send)
                return response(res, 'successfully upload dokumen', { upload })
              } else {
                return response(res, 'failed to upload dokumen', {}, 404, false)
              }
            } else {
              return response(res, 'failed to upload dokumen', {}, 404, false)
            }
          }
        }
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
  },
  editUploadDocument: async (req, res) => {
    const id = req.params.id
    const level = req.user.level
    uploadHelper(req, res, async function (err) {
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
        let dokumen = ''
        for (let x = 0; x < req.files.length; x++) {
          const path = `assets/documents/${req.files[x].filename}`
          dokumen += path + ', '
          if (x === req.files.length - 1) {
            dokumen = dokumen.slice(0, dokumen.length - 2)
          }
        }
        if (level === 4 || level === 5) {
          const valid = await Path.findByPk(id)
          if (valid) {
            const send = { path: dokumen }
            await valid.update(send)
            return response(res, 'successfully upload dokumen', { send })
          } else {
            return response(res, 'failed to edit upload dokumen', {}, 404, false)
          }
        } else {
          return response(res, "you can't edit dokumen", {}, 404, false)
        }
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
  },
  getActivity: async (req, res) => {
    try {
      const level = req.user.level
      const kode = req.user.kode
      //   const time = moment().utc().format('YYYY-MM-DD')
      let { limit, page, search, sort, typeSort } = req.query
      let searchValue = ''
      let sortValue = ''
      let typeSortValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || 'daily'
      }
      if (typeof sort === 'object') {
        sortValue = Object.values(sort)[0]
      } else {
        sortValue = sort || 'id'
      }
      if (typeof typeSort === 'object') {
        typeSortValue = Object.values(typeSort)[0]
      } else {
        typeSortValue = typeSort || 'DESC'
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
      if (level === 4) {
        const result = await activity.findAndCountAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'sa' }
            ],
            jenis_dokumen: searchValue
          },
          include: [
            {
              model: Path,
              as: 'doc'
            }
          ],
          order: [[sortValue, typeSortValue]],
          limit: limit,
          offset: (page - 1) * limit
        })
        const pageInfo = pagination('/dashboard/activity', req.query, page, limit, result.count)
        if (result) {
          return response(res, 'list activity', { result, pageInfo })
        } else {
          return response(res, 'failed to get activity', {}, 404, false)
        }
      } else if (level === 5) {
        const result = await activity.findAndCountAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'kasir' }
            ],
            jenis_dokumen: searchValue
          },
          include: [
            {
              model: Path,
              as: 'doc'
            }
          ],
          order: [[sortValue, typeSortValue]],
          limit: limit,
          offset: (page - 1) * limit
        })
        const pageInfo = pagination('/dashboard/activity', req.query, page, limit, result.count)
        if (result) {
          return response(res, 'list activity', { result, pageInfo })
        } else {
          return response(res, 'failed to get activity', {}, 404, false)
        }
      } else if (level === 1 || level === 2 || level === 3) {
        const result = await activity.findAndCountAll({
          where: {
            jenis_dokumen: searchValue
          },
          include: [
            {
              model: Path,
              as: 'doc'
            }
          ],
          order: [[sortValue, typeSortValue]],
          limit: limit,
          offset: (page - 1) * limit
        })
        const pageInfo = pagination('/dashboard/activity', req.query, page, limit, result.count)
        if (result) {
          return response(res, 'list activity', { result, pageInfo })
        } else {
          return response(res, 'failed to get activity', {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  approveDocument: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      const idAct = req.params.idAct
      if (level === 1 || level === 2 || level === 3) {
        const result = await Path.findByPk(id)
        const approve = { status_dokumen: 3 }
        if (result) {
          if (result.status_dokumen === 3) {
            return response(res, 'succes approve dokumen')
          } else {
            await result.update(approve)
            const act = await activity.findByPk(idAct)
            if (act) {
              const send = { progress: act.progress + 1 }
              await act.update(send)
              return response(res, 'succes approve dokumen')
            } else {
              return response(res, 'failed approve dokumen', {}, 404, false)
            }
          }
        } else {
          return response(res, 'failed approve dokumen', {}, 404, false)
        }
      } else {
        return response(res, "you're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  rejectDocument: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      const idAct = req.params.idAct
      const schema = joi.object({
        alasan: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 404, false)
      } else {
        if (level === 1 || level === 2 || level === 3) {
          const result = await Path.findByPk(id)
          const send = {
            alasan: results.alasan,
            status_dokumen: 0
          }
          if (result) {
            if (result.status_dokumen === 3) {
              await result.update(send)
              const act = await activity.findByPk(idAct)
              if (act) {
                const desc = { progress: act.progress - 1 }
                await act.update(desc)
                return response(res, 'success reject dokumen')
              } else {
                return response(res, 'failed reject dokumen', {}, 404, false)
              }
            } else {
              await result.update(send)
              return response(res, 'success reject dokumen')
            }
          } else {
            return response(res, 'failed reject dokumen', {}, 404, false)
          }
        } else {
          return response(res, "you're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
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
  sendMail: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      if (level === 1 || level === 2 || level === 3) {
        const dok = await Path.findByPk(id)
        if (dok) {
          const act = await activity.findByPk(dok.activityId)
          if (act) {
            const result = await email.findOne({
              where: {
                [Op.and]: [
                  { kode_plant: act.kode_plant },
                  { tipe: act.tipe }
                ]
              }
            })
            if (result) {
              const find = await depo.findOne({
                where: {
                  kode_plant: result.kode_plant
                }
              })
              if (find) {
                const mailOptions = {
                  from: `${result.email_ho_pic}`,
                  replyTo: `${result.email_ho_pic}`,
                  to: `${result.email_aos}`,
                  cc: `${result.email_sa_kasir}`,
                  subject: 'Rejected Dokumen',
                  html: `<body>
                  <div style="margin-top: 20px; margin-bottom: 20px;">Dear Bapak/Ibu AOS</div>
                  <div style="margin-bottom: 10px;">Report has been verified by Team Accounting with the following list:</div>
                  <table style="border-collapse: collapse; margin-bottom: 20px;">
                        <tr style="height: 75px;">
                          <th style="border: 1px solid black; background-color: lightgray; width: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">No</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Nama Area</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Nama File</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Jenis Report</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Tanggal Report</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Tanggal Upload</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Tanggal Verifikasi</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Status</th>
                          <th style="border: 1px solid black; background-color: lightgray; width: 100px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;">Alasan</th>
                        </tr>
                        <tr style="height: 50px;">
                          <th scope="row" style='border: 1px solid black;'>1</th>
                          <td style='border: 1px solid black;'>${find.nama_depo}</td>
                          <td style='border: 1px solid black;'>${dok.dokumen}</td>
                          <td style='border: 1px solid black;'>${act.jenis_dokumen}</td>
                          <td style='border: 1px solid black;'>${moment(act.createdAt).subtract(1, 'day').format('DD-MM-YYYY')}</td>
                          <td style='border: 1px solid black;'>${moment(dok.createdAt).format('DD-MM-YYYY')}</td>
                          <td style='border: 1px solid black;'>${moment(dok.updatedAt).format('DD-MM-YYYY')}</td>
                          <td style='border: 1px solid black;'>Rejected</td>
                          <td style='border: 1px solid black;'>${dok.alasan}</td>
                        </tr>
                  </table>
                  <a href="http://google.com">With the following link</a>
                  <div style="margin-top: 20px;">Thank you.</div>
              </body>
                  `
                }
                mailer.sendMail(mailOptions, (error, result) => {
                  if (error) {
                    return response(res, 'failed to send email', { error: error }, 401, false)
                  } else if (result) {
                    return response(res, 'success send email', { result: result })
                  }
                })
              } else {
                return response(res, 'failed to send email', {}, 401, false)
              }
            } else {
              return response(res, 'kode plant not found', {}, 401, false)
            }
          } else {
            return response(res, 'failed to send email', { }, 401, false)
          }
        } else {
          return response(res, 'failed to send email', { }, 401, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  reminder: async (req, res) => {
    try {
      const result = await depo.findAndCountAll({
        include: [{
          model: email,
          as: 'emails'
        }]
      })
      if (result) {
        console.log('success')
      } else {
        console.log('failed')
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  reportDokumen: async (req, res) => {
    // try {
    const level = req.user.level
    const { from, to, tipe } = req.query
    let tipeValue = ''
    let timeFrom = ''
    let timeTo = ''
    if (typeof from === 'object') {
      timeFrom = Object.values(from)[0]
    } else {
      timeFrom = from || ''
    }
    if (typeof to === 'object') {
      timeTo = Object.values(to)[0]
    } else {
      timeTo = to || ''
    }
    if (typeof tipe === 'object') {
      tipeValue = Object.values(tipe)[0]
    } else {
      tipeValue = tipe || 'daily'
    }
    const now = timeFrom === '' ? new Date(moment().utc().format('YYYY-MM-DD')) : new Date(moment(timeFrom).utc().format('YYYY-MM-DD'))
    const tomo = timeTo === '' ? new Date(moment().utc().format('YYYY-MM-DD 24:00:00')) : new Date(moment(timeTo).utc().format('YYYY-MM-DD 24:00:00'))
    const schema = joi.object({
      kode_plant: joi.string(),
      pic: joi.string()
    })
    const { value: results, error } = schema.validate(req.body)
    if (error) {
      return response(res, 'Error', { error: error.message }, 404, false)
    } else {
      if (level === 1) {
        if (results.pic) {
          const findPic = await pic.findAll({
            where: {
              pic: { [Op.like]: `%${results.pic}%` }
            }
          })
          if (findPic) {
            const depos = []
            findPic.map(x => {
              return (
                depos.push(x)
              )
            })
            console.log(depos[0].kode_depo)
            // response(res, 'list dokumen', { findPic })
            if (depos.length > 0) {
              const sa = []
              const kasir = []
              for (let i = 0; i < depos.length; i++) {
                const result = await depo.findAll({
                  where: {
                    kode_plant: depos[i].kode_depo
                  },
                  include: [
                    {
                      model: activity,
                      as: 'active',
                      where: {
                        [Op.and]: [
                          { kode_plant: depos[i].kode_depo },
                          { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                          { tipe: 'sa' }
                        ],
                        createdAt: {
                          [Op.lt]: tomo,
                          [Op.gt]: now
                        }
                      },
                      include: [
                        {
                          model: Path,
                          as: 'doc',
                          limit: 50
                        }
                      ]
                    },
                    {
                      model: documents,
                      as: 'dokumen',
                      where: {
                        [Op.and]: [
                          { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                          { uploadedBy: 'sa' }
                        ],
                        [Op.not]: { status: 'inactive' }
                      }
                    }
                  ]
                })
                if (result.length > 0) {
                  sa.push(result[0])
                }
              }
              for (let i = 0; i < depos.length; i++) {
                const result = await depo.findAll({
                  where: {
                    kode_plant: depos[i].kode_depo
                  },
                  include: [
                    {
                      model: activity,
                      as: 'active',
                      where: {
                        [Op.and]: [
                          { kode_plant: depos[i].kode_depo },
                          { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                          { tipe: 'kasir' }
                        ],
                        createdAt: {
                          [Op.lt]: tomo,
                          [Op.gt]: now
                        }
                      },
                      include: [
                        {
                          model: Path,
                          as: 'doc',
                          limit: 50
                        }
                      ]
                    },
                    {
                      model: documents,
                      as: 'dokumen',
                      where: {
                        [Op.and]: [
                          { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                          { uploadedBy: 'kasir' }
                        ],
                        [Op.not]: { status: 'inactive' }
                      }
                    }
                  ]
                })
                if (result.length > 0) {
                  kasir.push(result[0])
                }
              }
              if (kasir.length > 0 || sa.length > 0) {
                const data = []
                const body = []
                sa.map(x => {
                  return (
                    data.push(x.dokumen.length === undefined ? 0 : x.dokumen.length)
                  )
                })
                kasir.map(x => {
                  return (
                    data.push(x.dokumen.length)
                  )
                })
                const resu = []
                for (let i = 0; i <= Math.max(...data) - 1; i++) {
                  resu.push(i)
                }
                const header = ['No', 'PIC', 'Nama Depo', 'Kode Plant', 'Profit Center', 'Kode SAP 1', 'Tanggal Dokumen', 'Tanggal Upload', resu.length !== 0 && resu.map(item => item + 1) + '', 'Jumlah File Upload', 'Persentase', 'Status']
                console.log(header)
                return response(res, 'list dokumen', { findPic, sa, kasir })
              } else {
                return response(res, 'list dokumen', { findPic, sa, kasir })
              }
            } else {
              return response(res, 'depo no found', {}, 404, false)
            }
          } else {
            return response(res, 'failed to get report', {}, 404, false)
          }
        } else if (results.kode_plant) {
          const result = await depo.findAll({
            where: {
              kode_plant: results.kode_plant
            },
            include: [
              {
                model: activity,
                as: 'active',
                where: {
                  [Op.and]: [
                    { kode_plant: results.kode_plant },
                    { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                  ],
                  createdAt: {
                    [Op.lt]: tomo,
                    [Op.gt]: now
                  }
                },
                include: [
                  {
                    model: Path,
                    as: 'doc',
                    limit: 50
                  }
                ]
              }
            ]
          })
          if (result) {
            const findPic = await pic.findOne({
              where: {
                kode_depo: results.kode_depo
              }
            })
            if (findPic) {
              return response(res, 'list dokumen', { findPic, result })
            } else {
              return response(res, 'failed to get report', {}, 404, false)
            }
          } else {
            return response(res, 'failed to get report', {}, 404, false)
          }
        }
      }
      // else if (level === 2) {

      // } else if (level === 3) {

      // } else if (level === 4 || level === 6) {

      // }
    }
    // } catch (error) {
    //   return response(res, error.message, {}, 500, false)
    // }
  }
}
