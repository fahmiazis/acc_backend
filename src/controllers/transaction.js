const { pagination } = require('../helpers/pagination')
const { documents, sequelize, Path, depo, activity, pic, email, notif, date_clossing } = require('../models')
const { Op, QueryTypes } = require('sequelize')
const response = require('../helpers/response')
const joi = require('joi')
const uploadHelper = require('../helpers/upload')
const multer = require('multer')
const fs = require('fs')
const vs = require('fs-extra')
const { APP_URL } = process.env
const mailer = require('../helpers/mailer')
const moment = require('moment')
const xlsx = require('xlsx')

module.exports = {
  dashboard: async (req, res) => {
    try {
      let { limit, page, search, sort, typeSort, time, tipe, find } = req.query
      let searchValue = ''
      let sortValue = ''
      let typeSortValue = ''
      let timeValue = ''
      let tipeValue = ''
      let findValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || ''
      }
      if (typeof find === 'object') {
        findValue = Object.values(find)[0]
      } else {
        findValue = find || ''
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
      let timeUser = new Date(moment().format('YYYY-MM-DD'))
      let timeUserTomo = new Date(moment().add(1, 'days').format('YYYY-MM-DD'))
      let now = timeValue === '' ? new Date(moment().format('YYYY-MM-DD')) : new Date(moment(timeValue).format('YYYY-MM-DD'))
      let tomo = timeValue === '' ? new Date(moment().add(1, 'days').format('YYYY-MM-DD')) : new Date(moment(timeValue).add(1, 'days').format('YYYY-MM-DD'))
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
                { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
              ],
              [Op.and]: [
                { status_depo: cabang },
                { uploadedBy: 'sa' },
                { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
              ],
              [Op.not]: { status: 'inactive' }
            },
            order: [[sortValue, typeSortValue]],
            limit: limit,
            offset: (page - 1) * limit
          })
          const pageInfo = pagination('/dokumen/get', req.query, page, limit, results.count)
          if (results) {
            if (tipeValue === 'monthly') {
              timeUser = new Date(moment().startOf('month').format('YYYY-MM-DD'))
              timeUserTomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
            }
            const cek = await activity.findAll({
              where: {
                [Op.and]: [
                  { kode_plant: kode },
                  { tipe: 'sa' },
                  { jenis_dokumen: tipeValue }
                ],
                createdAt: {
                  [Op.gt]: timeUser,
                  [Op.lt]: timeUserTomo
                }
              }
            })
            if (cek.length > 0) {
              return response(res, 'list dokumen', { results, pageInfo })
            } else {
              if (tipeValue === 'daily') {
                const now = new Date(moment().startOf('month').format('YYYY-MM-DD'))
                const tomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
                const find = await activity.findAll({
                  where: {
                    [Op.and]: [
                      { kode_plant: kode },
                      { tipe: 'sa' },
                      { jenis_dokumen: tipeValue }
                    ],
                    createdAt: {
                      [Op.lt]: tomo,
                      [Op.gt]: now
                    }
                  }
                })
                if (find) {
                  const temp = []
                  find.map(item => {
                    return temp.push(item.id)
                  })
                  for (let i = 0; i < find.length; i++) {
                    const send = {
                      access: 'lock'
                    }
                    const change = await activity.findByPk(temp[i])
                    if (change) {
                      await change.update(send)
                    }
                  }
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
                    return response(res, 'list dokumen', { results, pageInfo, cek: cek })
                  } else {
                    return response(res, 'failed to get dokumen', {}, 404, false)
                  }
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
                    return response(res, 'list dokumen', { results, pageInfo, cek: cek })
                  } else {
                    return response(res, 'failed to get dokumen', {}, 404, false)
                  }
                }
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
                  return response(res, 'list dokumen', { results, pageInfo, cek: cek })
                } else {
                  return response(res, 'failed to get dokumen', {}, 404, false)
                }
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
                { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
              ],
              [Op.and]: [
                { status_depo: cabang },
                { uploadedBy: 'kasir' },
                { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
              ],
              [Op.not]: { status: 'inactive' }
            },
            order: [[sortValue, typeSortValue]]
          })
          const pageInfo = pagination('/dokumen/get', req.query, page, limit, results.count)
          if (results) {
            if (tipeValue === 'monthly') {
              timeUser = new Date(moment().startOf('month').format('YYYY-MM-DD'))
              timeUserTomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
            }
            const cek = await activity.findAll({
              where: {
                [Op.and]: [
                  { kode_plant: kode },
                  { tipe: 'kasir' },
                  { jenis_dokumen: tipeValue }
                ],
                createdAt: {
                  [Op.lt]: timeUserTomo,
                  [Op.gt]: timeUser
                }
              }
            })
            if (cek.length > 0) {
              return response(res, 'list dokumen', { results, pageInfo })
            } else {
              if (tipeValue === 'daily') {
                const now = new Date(moment().clone().startOf('month').format('YYYY-MM-DD'))
                const tomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
                const find = await activity.findAll({
                  where: {
                    [Op.and]: [
                      { kode_plant: kode },
                      { tipe: 'kasir' },
                      { jenis_dokumen: tipeValue }
                    ],
                    createdAt: {
                      [Op.lt]: tomo,
                      [Op.gt]: now
                    }
                  }
                })
                if (find) {
                  const temp = []
                  find.map(item => {
                    return temp.push(item.id)
                  })
                  for (let i = 0; i < find.length; i++) {
                    const send = {
                      access: 'lock'
                    }
                    const change = await activity.findByPk(temp[i])
                    if (change) {
                      await change.update(send)
                    }
                  }
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
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
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
            if (tipeValue === 'monthly') {
              now = new Date(moment(timeValue).startOf('month').format('YYYY-MM-DD'))
              tomo = new Date(moment(timeValue).endOf('month').format('YYYY-MM-DD'))
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
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
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
            if (tipeValue === 'monthly') {
              now = new Date(moment(timeValue).startOf('month').format('YYYY-MM-DD'))
              tomo = new Date(moment(timeValue).endOf('month').format('YYYY-MM-DD'))
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
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
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
            if (tipeValue === 'monthly') {
              now = new Date(moment(timeValue).startOf('month').format('YYYY-MM-DD'))
              tomo = new Date(moment(timeValue).endOf('month').format('YYYY-MM-DD'))
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
    const idAct = req.params.idAct
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
          if (level === 5) {
            const result = await documents.findByPk(id)
            if (result) {
              const active = await activity.findByPk(idAct)
              if (active.access === 'lock') {
                return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
              } else {
                if (result.jenis_dokumen === 'daily') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'daily'
                    }
                  })
                  if (moment().format('HH:mm') < moment(time.time).format('HH:mm')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                } else if (result.jenis_dokumen === 'monthly') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'monthly'
                    }
                  })
                  if (moment().format('LL') < moment(time.day).format('LL')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                }
              }
            } else {
              return response(res, 'failed to upload dokumen', {}, 404, false)
            }
          } else if (level === 4) {
            const result = await documents.findByPk(id)
            if (result) {
              const active = await activity.findByPk(idAct)
              if (active.access === 'lock') {
                return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
              } else {
                if (result.jenis_dokumen === 'daily') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'daily'
                    }
                  })
                  if (moment().format('HH:mm') < moment(time.time).format('HH:mm')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                } else if (result.jenis_dokumen === 'monthly') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'monthly'
                    }
                  })
                  if (moment().format('LL') < moment(time.day).format('LL')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                }
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
    const idAct = req.params.idAct
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
        const dokumen = `assets/documents/${req.file.filename}`
        if (level === 4 || level === 5) {
          const valid = await Path.findByPk(id)
          if (valid) {
            const active = await activity.findByPk(idAct)
            if (active.access === 'lock') {
              return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
            } else {
              const send = { path: dokumen }
              await valid.update(send)
              return response(res, 'successfully upload dokumen', { send })
            }
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
        limit = 30
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
    // try {
    const level = req.user.level
    const id = req.params.id
    const idAct = req.params.idAct
    if (level === 1 || level === 2 || level === 3) {
      const result = await Path.findByPk(id)
      let approve = { status_dokumen: 3 }
      if (result) {
        if (result.status_dokumen === 3 || result.status_dokumen === 5) {
          const find = await notif.findOne({
            where: {
              [Op.or]: [
                { upload: 'true' },
                { reject: 'true' }
              ],
              pathId: id
            }
          })
          if (find) {
            await find.destroy()
            return response(res, 'succes approve dokumen')
          } else {
            return response(res, 'succes approve dokumen')
          }
        } else if (result.status_dokumen === 4 || result.status_dokumen === 6) {
          approve = { status_dokumen: 5 }
          await result.update(approve)
          const act = await activity.findByPk(idAct)
          if (act) {
            const send = { progress: act.progress + 1 }
            await act.update(send)
            const find = await notif.findOne({
              where: {
                [Op.or]: [
                  { upload: 'true' },
                  { reject: 'true' }
                ],
                pathId: id
              }
            })
            if (find) {
              await find.destroy()
              return response(res, 'succes approve dokumen')
            } else {
              return response(res, 'succes approve dokumen')
            }
          } else {
            return response(res, 'failed approve dokumen', {}, 404, false)
          }
        } else {
          approve = { status_dokumen: 3 }
          await result.update(approve)
          const act = await activity.findByPk(idAct)
          if (act) {
            const send = { progress: act.progress + 1 }
            await act.update(send)
            const find = await notif.findOne({
              where: {
                [Op.or]: [
                  { upload: 'true' },
                  { reject: 'true' }
                ],
                pathId: id
              }
            })
            if (find) {
              await find.destroy()
              return response(res, 'succes approve dokumen')
            } else {
              return response(res, 'succes approve dokumen')
            }
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
    // } catch (error) {
    //   return response(res, error.message, {}, 500, false)
    // }
  },
  rejectDocument: async (req, res) => {
    // try {
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
        let send = {
          alasan: results.alasan,
          status_dokumen: 0
        }
        if (result) {
          if (result.status_dokumen === 3) {
            send = {
              alasan: results.alasan,
              status_dokumen: 0
            }
            await result.update(send)
            const act = await activity.findByPk(idAct)
            if (act) {
              const desc = { progress: act.progress - 1 }
              const update = await act.update(desc)
              if (update) {
                const data = {
                  kode_plant: act.kode_plant,
                  reject: 'true',
                  upload: 'false',
                  activityId: idAct,
                  pathId: id,
                  tipe: act.tipe
                }
                const find = await notif.findOne({
                  where: {
                    [Op.or]: [
                      { upload: 'true' },
                      { reject: 'true' }
                    ],
                    pathId: id
                  }
                })
                if (find) {
                  await notif.create(data)
                  return response(res, 'success reject dokumen')
                } else {
                  await notif.create(data)
                  return response(res, 'success reject dokumen')
                }
              } else {
                return response(res, 'failed reject dokumen', {}, 404, false)
              }
            } else {
              return response(res, 'failed reject dokumen', {}, 404, false)
            }
          } else if (result.status_dokumen === 5) {
            send = {
              alasan: results.alasan,
              status_dokumen: 6
            }
            await result.update(send)
            const act = await activity.findByPk(idAct)
            if (act) {
              const desc = { progress: act.progress - 1 }
              const update = await act.update(desc)
              if (update) {
                const data = {
                  kode_plant: act.kode_plant,
                  reject: 'true',
                  upload: 'false',
                  activityId: idAct,
                  pathId: id,
                  tipe: act.tipe
                }
                const find = await notif.findOne({
                  where: {
                    [Op.or]: [
                      { upload: 'true' },
                      { reject: 'true' }
                    ],
                    pathId: id
                  }
                })
                if (find) {
                  await notif.create(data)
                  return response(res, 'success reject dokumen')
                } else {
                  await notif.create(data)
                  return response(res, 'success reject dokumen')
                }
              } else {
                return response(res, 'failed reject dokumen', {}, 404, false)
              }
            } else {
              return response(res, 'failed reject dokumen', {}, 404, false)
            }
          } else if (result.status_dokumen === 6 || result.status_dokumen === 0) {
            return response(res, 'success reject dokumen')
          } else {
            send = {
              alasan: results.alasan,
              status_dokumen: 0
            }
            const update = await result.update(send)
            const act = await activity.findByPk(idAct)
            if (act && update) {
              const data = {
                kode_plant: act.kode_plant,
                reject: 'true',
                upload: 'false',
                activityId: idAct,
                pathId: id,
                tipe: act.tipe
              }
              const find = await notif.findOne({
                where: {
                  [Op.or]: [
                    { upload: 'true' },
                    { reject: 'true' }
                  ],
                  pathId: id
                }
              })
              if (find) {
                await notif.create(data)
                return response(res, 'success reject dokumen')
              } else {
                await notif.create(data)
                return response(res, 'success reject dokumen')
              }
            } else {
              return response(res, 'failed reject dokumen', {}, 404, false)
            }
          }
        } else {
          return response(res, 'failed reject dokumen', {}, 404, false)
        }
      } else {
        return response(res, "you're not super administrator", {}, 404, false)
      }
    }
    // } catch (error) {
    //   return response(res, error.message, {}, 500, false)
    // }
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
                  cc: `${result.email_sa_kasir}, ${result.email_ho_pic}`,
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
                  <a href="http://trial.pinusmerahabadi.co.id:3000/">With the following link</a>
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
  reportDokumen: async (req, res) => {
    // try {
    const level = req.user.level
    const depoKode = req.user.kode
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
    let now = timeFrom === '' ? new Date(moment().format('YYYY-MM-DD')) : new Date(moment(timeFrom).format('YYYY-MM-DD'))
    let tomo = timeTo === '' ? new Date(moment().format('YYYY-MM-DD 24:00:00')) : new Date(moment(timeTo).format('YYYY-MM-DD 24:00:00'))
    if (tipeValue === 'monthly') {
      now = new Date(moment(timeFrom).startOf('month').format('YYYY-MM-DD'))
      tomo = new Date(moment(timeTo).endOf('month').format('YYYY-MM-DD'))
    }
    const schema = joi.object({
      kode_plant: joi.string().allow(''),
      pic: joi.string().allow('')
    })
    const { value: results, error } = schema.validate(req.body)
    if (error) {
      return response(res, 'Error', { error: error.message }, 404, false)
    } else {
      const first = ['No', 'GROM', 'PIC Acc', 'Kode Plant', 'Nama Area', 'Profit Center', 'Kode SAP 1', 'Status Depo']
      const last = ['Total Laporan', 'Dokumen Yang Telah Diverifikasi', 'Persantase']
      if (level === 1 || level === 2 || level === 3) {
        if (results.pic !== '') {
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
                    },
                    {
                      model: documents,
                      as: 'dokumen',
                      where: {
                        jenis_dokumen: { [Op.like]: `%${tipeValue}%` }
                      }
                    }
                  ]
                })
                if (result.length > 0) {
                  sa.push(result[0])
                }
              }
              if (kasir.length > 0 || sa.length > 0) {
                const dokumen = await documents.findAll({
                  where: {
                    jenis_dokumen: { [Op.like]: `%${tipeValue}%` }
                  }
                })
                if (dokumen.length > 0) {
                  const resu = []
                  dokumen.map(item => {
                    return (
                      resu.push(item.nama_dokumen)
                    )
                  })
                  const obj = {}
                  const resultDokumen = []
                  resu.forEach(item => {
                    if (!obj[item]) { obj[item] = 0 }
                    obj[item] += 1
                  })
                  for (const prop in obj) {
                    if (obj[prop] >= 2 || obj[prop] === 1) {
                      resultDokumen.push(prop)
                    }
                  }
                  const temp = []
                  for (let i = 0; i < sa.length; i++) {
                    for (let j = 0; j < sa[i].active.length; j++) {
                      if (tipeValue === 'monthly') {
                        temp.push(`${sa[i].kode_plant} ${moment(sa[i].active[j].createdAt).format('YYYY/MM')}`)
                      } else {
                        temp.push(`${sa[i].kode_plant} ${moment(sa[i].active[j].createdAt).format('YYYY/MM/DD')}`)
                      }
                    }
                  }
                  const object = {}
                  const result = []
                  temp.forEach(item => {
                    if (!object[item]) { object[item] = 0 }
                    object[item] += 1
                  })
                  for (const prop in object) {
                    if (object[prop] >= 2 || object[prop] === 1) {
                      result.push(prop)
                    }
                  }
                  const dataSa = []
                  for (let i = 0; i < result.length; i++) {
                    const kode = result[i].split(' ')
                    let begin = new Date(moment(kode[1]).format('YYYY-MM-DD'))
                    let last = new Date(moment(kode[1]).format('YYYY-MM-DD 24:00:00'))
                    if (tipeValue === 'monthly') {
                      begin = new Date(moment(timeFrom).startOf('month').format('YYYY-MM-DD'))
                      last = new Date(moment(timeTo).endOf('month').format('YYYY-MM-DD'))
                    }
                    const hasil = await depo.findAll({
                      where: {
                        kode_plant: kode[0]
                      },
                      include: [
                        {
                          model: activity,
                          as: 'active',
                          where: {
                            [Op.and]: [
                              { kode_plant: kode[0] },
                              { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                            ],
                            createdAt: {
                              [Op.lt]: last,
                              [Op.gt]: begin
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
                            jenis_dokumen: { [Op.like]: `%${tipeValue}%` }
                          }
                        }
                      ]
                    })
                    if (hasil.length > 0) {
                      dataSa.push(hasil[0])
                    }
                  }
                  const saBody = []
                  for (let i = 0; i < dataSa.length; i++) {
                    const temp = []
                    temp.push(i + 1)
                    temp.push(dataSa[i].nama_grom)
                    temp.push(dataSa[i].nama_pic_1)
                    temp.push(dataSa[i].kode_plant)
                    temp.push(dataSa[i].nama_depo)
                    temp.push(dataSa[i].profit_center)
                    temp.push(dataSa[i].kode_sap_1)
                    temp.push(dataSa[i].status_depo)
                    for (let d = 0; d < resultDokumen.length; d++) {
                      if (dataSa[i].active[0] !== undefined && dataSa[i].active[1] !== undefined) {
                        if (dataSa[i].active[0].doc.length > 0 || dataSa[i].active[1].doc.length > 0) {
                          const iter = dataSa[i].active[1] !== undefined ? dataSa[i].active[1].doc.length : 0
                          const collect = []
                          for (let j = 0; j < dataSa[i].active[0].doc.length; j++) {
                            if (dataSa[i].active[0].doc[j].dokumen === resultDokumen[d]) {
                              dataSa[i].active[0].doc[j].status_dokumen === 3
                                ? collect.push(moment(dataSa[i].active[0].doc[j].createdAt).format('LLL'))
                                : dataSa[i].active[0].doc[j].status_dokumen === 5
                                  ? collect.push('Telat  ' + `(${moment(dataSa[i].active[0].doc[j].createdAt).format('LLL')})`)
                                  : collect.push('-')
                            }
                          }
                          for (let x = 0; x < iter; x++) {
                            if (dataSa[i].active[1].doc[x].dokumen === resultDokumen[d]) {
                              dataSa[i].active[1].doc[x].status_dokumen === 3
                                ? collect.push(moment(dataSa[i].active[1].doc[x].createdAt).format('LLL'))
                                : dataSa[i].active[1].doc[x].status_dokumen === 5
                                  ? collect.push('Telat  ' + `(${moment(dataSa[i].active[1].doc[x].createdAt).format('LLL')})`)
                                  : collect.push('-')
                            }
                          }
                          if (collect.length > 0) {
                            temp.push(collect[0])
                          } else {
                            temp.push('-')
                          }
                        } else {
                          temp.push('-')
                        }
                      } else if (dataSa[i].active[0] !== undefined) {
                        const collect = []
                        for (let j = 0; j < dataSa[i].active[0].doc.length; j++) {
                          if (dataSa[i].active[0].doc[j].dokumen === resultDokumen[d]) {
                            dataSa[i].active[0].doc[j].status_dokumen === 3
                              ? collect.push(moment(dataSa[i].active[0].doc[j].createdAt).format('LLL'))
                              : dataSa[i].active[0].doc[j].status_dokumen === 5
                                ? collect.push('Telat  ' + `(${moment(dataSa[i].active[0].doc[j].createdAt).format('LLL')})`)
                                : collect.push('-')
                          }
                        }
                        if (collect.length > 0) {
                          temp.push(collect[0])
                        } else {
                          temp.push('-')
                        }
                      }
                    }
                    temp.push(dataSa[i].dokumen.length)
                    if (dataSa[i].active[1] !== undefined) {
                      temp.push(dataSa[i].active[0].progress + dataSa[i].active[1].progress)
                      temp.push(Math.round(((dataSa[i].active[0].progress + dataSa[i].active[1].progress) / dataSa[i].dokumen.length) * 100) + '%')
                    } else {
                      temp.push(dataSa[i].active[0].progress)
                      temp.push(Math.round((dataSa[i].active[0].progress / dataSa[i].dokumen.length) * 100) + '%')
                    }
                    saBody.push(temp)
                  }
                  const header = first.concat(resultDokumen, last)
                  const body = [header, ...saBody]
                  const wb = xlsx.utils.book_new()
                  const name = new Date().getTime().toString().concat('.xlsx')
                  wb.Props = {
                    Title: 'Report',
                    Author: 'Team Accounting',
                    CreatedDate: new Date()
                  }
                  const ws = xlsx.utils.aoa_to_sheet(body)
                  wb.Sheets['Sheet 1'] = ws
                  xlsx.utils.book_append_sheet(wb, ws, 'Results')
                  await xlsx.writeFile(wb, name, { type: 'file' })
                  vs.move(name, `assets/exports/${name}`, function (err) {
                    if (err) {
                      throw err
                    }
                    console.log('success')
                  })
                  return response(res, 'success', { link: `${APP_URL}/download/${name}` })
                } else {
                  return response(res, 'list dokumen', { findPic, sa, kasir })
                }
              }
            } else {
              return response(res, 'depo not found', {}, 404, false)
            }
          } else {
            return response(res, 'failed to get report', {}, 404, false)
          }
        } else if (results.kode_plant !== '') {
          const sa = await depo.findAll({
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
              },
              {
                model: documents,
                as: 'dokumen',
                where: {
                  jenis_dokumen: { [Op.like]: `%${tipeValue}%` }
                }
              }
            ]
          })
          if (sa.length > 0) {
            const dokumen = await documents.findAll({
              where: {
                jenis_dokumen: { [Op.like]: `%${tipeValue}%` }
              }
            })
            if (dokumen.length > 0) {
              const resu = []
              dokumen.map(item => {
                return (
                  resu.push(item.nama_dokumen)
                )
              })
              const obj = {}
              const resultDokumen = []
              resu.forEach(item => {
                if (!obj[item]) { obj[item] = 0 }
                obj[item] += 1
              })
              for (const prop in obj) {
                if (obj[prop] >= 2 || obj[prop] === 1) {
                  resultDokumen.push(prop)
                }
              }
              const temp = []
              for (let i = 0; i < sa.length; i++) {
                for (let j = 0; j < sa[i].active.length; j++) {
                  if (tipeValue === 'monthly') {
                    temp.push(`${sa[i].kode_plant} ${moment(sa[i].active[j].createdAt).format('YYYY/MM')}`)
                  } else {
                    temp.push(`${sa[i].kode_plant} ${moment(sa[i].active[j].createdAt).format('YYYY/MM/DD')}`)
                  }
                }
              }
              const object = {}
              const result = []
              temp.forEach(item => {
                if (!object[item]) { object[item] = 0 }
                object[item] += 1
              })
              for (const prop in object) {
                if (object[prop] >= 2 || object[prop] === 1) {
                  result.push(prop)
                }
              }
              const dataSa = []
              for (let i = 0; i < result.length; i++) {
                const kode = result[i].split(' ')
                let begin = new Date(moment(kode[1]).format('YYYY-MM-DD'))
                let last = new Date(moment(kode[1]).format('YYYY-MM-DD 24:00:00'))
                if (tipeValue === 'monthly') {
                  begin = new Date(moment(timeFrom).startOf('month').format('YYYY-MM-DD'))
                  last = new Date(moment(timeTo).endOf('month').format('YYYY-MM-DD'))
                }
                const hasil = await depo.findAll({
                  where: {
                    kode_plant: kode[0]
                  },
                  include: [
                    {
                      model: activity,
                      as: 'active',
                      where: {
                        [Op.and]: [
                          { kode_plant: kode[0] },
                          { jenis_dokumen: tipeValue }
                        ],
                        createdAt: {
                          [Op.lt]: last,
                          [Op.gt]: begin
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
                        jenis_dokumen: tipeValue
                      }
                    }
                  ]
                })
                if (hasil.length > 0) {
                  dataSa.push(hasil[0])
                }
              }
              const saBody = []
              for (let i = 0; i < dataSa.length; i++) {
                const temp = []
                temp.push(i + 1)
                temp.push(dataSa[i].nama_grom)
                temp.push(dataSa[i].nama_pic_1)
                temp.push(dataSa[i].kode_plant)
                temp.push(dataSa[i].nama_depo)
                temp.push(dataSa[i].profit_center)
                temp.push(dataSa[i].kode_sap_1)
                temp.push(dataSa[i].status_depo)
                for (let d = 0; d < resultDokumen.length; d++) {
                  if (dataSa[i].active[0] !== undefined && dataSa[i].active[1] !== undefined) {
                    if (dataSa[i].active[0].doc.length > 0 || dataSa[i].active[1].doc.length > 0) {
                      const iter = dataSa[i].active[1] !== undefined ? dataSa[i].active[1].doc.length : 0
                      const collect = []
                      for (let j = 0; j < dataSa[i].active[0].doc.length; j++) {
                        if (dataSa[i].active[0].doc[j].dokumen === resultDokumen[d]) {
                          dataSa[i].active[0].doc[j].status_dokumen === 3
                            ? collect.push(moment(dataSa[i].active[0].doc[j].createdAt).format('LLL'))
                            : dataSa[i].active[0].doc[j].status_dokumen === 5
                              ? collect.push('Telat  ' + `(${moment(dataSa[i].active[0].doc[j].createdAt).format('LLL')})`)
                              : collect.push('-')
                        }
                      }
                      for (let x = 0; x < iter; x++) {
                        if (dataSa[i].active[1].doc[x].dokumen === resultDokumen[d]) {
                          dataSa[i].active[1].doc[x].status_dokumen === 3
                            ? collect.push(moment(dataSa[i].active[1].doc[x].createdAt).format('LLL'))
                            : dataSa[i].active[1].doc[x].status_dokumen === 5
                              ? collect.push('Telat  ' + `(${moment(dataSa[i].active[1].doc[x].createdAt).format('LLL')})`)
                              : collect.push('-')
                        }
                      }
                      if (collect.length > 0) {
                        temp.push(collect[0])
                      } else {
                        temp.push('-')
                      }
                    } else {
                      temp.push('-')
                    }
                  } else if (dataSa[i].active[0] !== undefined) {
                    const collect = []
                    for (let j = 0; j < dataSa[i].active[0].doc.length; j++) {
                      if (dataSa[i].active[0].doc[j].dokumen === resultDokumen[d]) {
                        dataSa[i].active[0].doc[j].status_dokumen === 3
                          ? collect.push(moment(dataSa[i].active[0].doc[j].createdAt).format('LLL'))
                          : dataSa[i].active[0].doc[j].status_dokumen === 5
                            ? collect.push('Telat  ' + `(${moment(dataSa[i].active[0].doc[j].createdAt).format('LLL')})`)
                            : collect.push('-')
                      }
                    }
                    if (collect.length > 0) {
                      temp.push(collect[0])
                    } else {
                      temp.push('-')
                    }
                  }
                }
                temp.push(dataSa[i].dokumen.length)
                if (dataSa[i].active[1] !== undefined) {
                  temp.push(dataSa[i].active[0].progress + dataSa[i].active[1].progress)
                  temp.push(Math.round(((dataSa[i].active[0].progress + dataSa[i].active[1].progress) / dataSa[i].dokumen.length) * 100) + '%')
                } else {
                  temp.push(dataSa[i].active[0].progress)
                  temp.push(Math.round((dataSa[i].active[0].progress / dataSa[i].dokumen.length) * 100) + '%')
                }
                saBody.push(temp)
              }
              const header = first.concat(resultDokumen, last)
              const body = [header, ...saBody]
              const wb = xlsx.utils.book_new()
              const name = new Date().getTime().toString().concat('.xlsx')
              wb.Props = {
                Title: 'Report',
                Author: 'Team Accounting',
                CreatedDate: new Date()
              }
              const ws = xlsx.utils.aoa_to_sheet(body)
              wb.Sheets['Sheet 1'] = ws
              xlsx.utils.book_append_sheet(wb, ws, 'Results')
              await xlsx.writeFile(wb, name, { type: 'file' })
              vs.move(name, `assets/exports/${name}`, function (err) {
                if (err) {
                  throw err
                }
                console.log('success')
              })
              return response(res, 'success', { link: `${APP_URL}/download/${name}` })
            } else {
              return response(res, 'failed to get report', {}, 404, false)
            }
          } else {
            return response(res, 'failed to get report', {}, 404, false)
          }
        }
      } else if (level === 4) {
        const sa = await depo.findAll({
          where: {
            kode_plant: depoKode
          },
          include: [
            {
              model: activity,
              as: 'active',
              where: {
                [Op.and]: [
                  { kode_plant: depoKode },
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
                ]
              }
            }
          ]
        })
        if (sa.length > 0) {
          const data = []
          sa.map(item => {
            return (
              data.push(item.dokumen.length)
            )
          })
          const resu = []
          for (let i = 1; i <= Math.max(...data); i++) {
            resu.push(`${i}`)
          }
          const saBody = []
          for (let i = 0; i < sa.length; i++) {
            for (let j = 0; j < sa[i].active.length; j++) {
              const temp = []
              temp.push(sa[i].active.indexOf(sa[i].active[j]) + 1)
              temp.push(sa[i].nama_depo)
              temp.push(sa[i].kode_plant)
              temp.push(sa[i].profit_center)
              temp.push(sa[i].kode_sap_1)
              temp.push(sa[i].status_depo)
              temp.push(moment(sa[i].active[j].documentDate).format('DD MMMM YYYY'))
              temp.push(moment(sa[i].active[j].createdAt).format('DD MMMM YYYY'))
              for (let d = 0; d < resu.length; d++) {
                const go = sa[i].dokumen[d] === undefined ? 'a' : sa[i].dokumen[d].nama_dokumen
                if (sa[i].active[j].doc.length > 0) {
                  if (sa[i].active[j].doc[d] !== undefined) {
                    sa[i].active[j].doc.find(({ dokumen }) => dokumen === go) !== undefined
                      ? temp.push(sa[i].active[j].doc[d].status_dokumen === 1 ? 'O' : sa[i].active[j].doc[d].status_dokumen === 0 ? 'X' : sa[i].active[j].doc[d].status_dokumen === 3 ? 'V' : sa[i].active[j].doc[d].status_dokumen === 2 ? 'O' : sa[i].active[j].doc[d].status_dokumen === 4 ? 'Telat kirim' : '-')
                      : temp.push('-')
                  } else {
                    temp.push('-')
                  }
                } else {
                  temp.push('-')
                }
              }
              temp.push(sa[i].dokumen.length)
              temp.push(Math.round((sa[i].active[j].progress / sa[i].dokumen.length) * 100) + '%')
              if (sa[i].active[j].doc.length === 0) {
                temp.push('Belum Upload')
              } else {
                temp.push(((sa[i].active[j].progress / sa[i].dokumen.length) * 100) === 100 ? 'Done' : ((sa[i].active[j].progress / sa[i].dokumen.length) * 100) < 100 ? 'Kurang Upload' : '')
              }
              temp.push(tipeValue)
              temp.push(sa[i].active[j].tipe)
              saBody.push(temp)
            }
          }
          const first = ['No', 'Nama Depo', 'Kode Plant', 'Profit Center', 'Kode SAP 1', 'Status Depo', 'Tanggal Dokumen', 'Tanggal Upload']
          const last = ['Jumlah File Upload', 'Persentase', 'Status', 'Jenis Dokumen', 'Uploaded By']
          const header = first.concat(resu, last)
          const body = [header, ...saBody]
          const wb = xlsx.utils.book_new()
          const name = new Date().getTime().toString().concat('.xlsx')
          wb.Props = {
            Title: 'Report',
            Author: 'Team Accounting',
            CreatedDate: new Date()
          }
          const ws = xlsx.utils.aoa_to_sheet(body)
          wb.Sheets['Sheet 1'] = ws
          xlsx.utils.book_append_sheet(wb, ws, 'Results')
          await xlsx.writeFile(wb, name, { type: 'file' })
          vs.move(name, `assets/exports/${name}`, function (err) {
            if (err) {
              throw err
            }
            console.log('success')
          })
          return response(res, 'success', { link: `${APP_URL}/download/${name}` })
        } else {
          return response(res, 'failed to get report', {}, 404, false)
        }
      } else if (level === 5) {
        const kasir = await depo.findAll({
          where: {
            kode_plant: depoKode
          },
          include: [
            {
              model: activity,
              as: 'active',
              where: {
                [Op.and]: [
                  { kode_plant: depoKode },
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
                ]
              }
            }
          ]
        })
        if (kasir.length > 0) {
          const data = []
          kasir.map(item => {
            return (
              data.push(item.dokumen.length)
            )
          })
          const resu = []
          for (let i = 1; i <= Math.max(...data); i++) {
            resu.push(`${i}`)
          }
          const kasirBody = []
          for (let i = 0; i < kasir.length; i++) {
            data.push(kasir[i].dokumen.length)
            for (let j = 0; j < kasir[i].active.length; j++) {
              const temp = []
              temp.push(kasir[i].active.indexOf(kasir[i].active[j]) + 1)
              temp.push(kasir[i].nama_depo)
              temp.push(kasir[i].kode_plant)
              temp.push(kasir[i].profit_center)
              temp.push(kasir[i].kode_sap_1)
              temp.push(kasir[i].status_depo)
              temp.push(moment(kasir[i].active[j].documentDate).format('DD MMMM YYYY'))
              temp.push(moment(kasir[i].active[j].createdAt).format('DD MMMM YYYY'))
              for (let d = 0; d < resu.length; d++) {
                if (kasir[i].active[j].doc.length > 0) {
                  const go = kasir[i].dokumen[d] === undefined ? 'a' : kasir[i].dokumen[d].nama_dokumen
                  if (kasir[i].active[j].doc[d] !== undefined) {
                    kasir[i].active[j].doc.find(({ dokumen }) => dokumen === go) !== undefined
                      ? temp.push(kasir[i].active[j].doc[d].status_dokumen === 1 ? 'O' : kasir[i].active[j].doc[d].status_dokumen === 0 ? 'X' : kasir[i].active[j].doc[d].status_dokumen === 3 ? 'V' : kasir[i].active[j].doc[d].status_dokumen === 2 ? 'O' : kasir[i].active[j].doc[d].status_dokumen === 4 ? 'Telat kirim' : '')
                      : temp.push('-')
                  } else {
                    temp.push('-')
                  }
                } else {
                  temp.push('-')
                }
              }
              temp.push(kasir[i].dokumen.length)
              temp.push(Math.round((kasir[i].active[j].progress / kasir[i].dokumen.length) * 100) + '%')
              if (kasir[i].active[j].doc.length === 0) {
                temp.push('Belum Upload')
              } else {
                temp.push(((kasir[i].active[j].progress / kasir[i].dokumen.length) * 100) === 100 ? 'Done' : ((kasir[i].active[j].progress / kasir[i].dokumen.length) * 100) < 100 ? 'Kurang Upload' : '')
              }
              temp.push(tipeValue)
              temp.push(kasir[i].active[j].tipe)
              kasirBody.push(temp)
            }
          }
          const first = ['No', 'Nama Depo', 'Kode Plant', 'Profit Center', 'Kode SAP 1', 'Status Depo', 'Tanggal Dokumen', 'Tanggal Upload']
          const last = ['Jumlah File Upload', 'Persentase', 'Status', 'Jenis Dokumen', 'Uploaded By']
          const header = first.concat(resu, last)
          const body = [header, ...kasirBody]
          const wb = xlsx.utils.book_new()
          const name = new Date().getTime().toString().concat('.xlsx')
          wb.Props = {
            Title: 'Report',
            Author: 'Team Accounting',
            CreatedDate: new Date()
          }
          const ws = xlsx.utils.aoa_to_sheet(body)
          wb.Sheets['Sheet 1'] = ws
          xlsx.utils.book_append_sheet(wb, ws, 'Results')
          await xlsx.writeFile(wb, name, { type: 'file' })
          vs.move(name, `assets/exports/${name}`, function (err) {
            if (err) {
              throw err
            }
            console.log('success')
          })
          return response(res, 'success', { link: `${APP_URL}/download/${name}` })
        } else {
          return response(res, 'failed to get report', {}, 404, false)
        }
      } else {
        return response(res, 'failed to get report', {}, 404, false)
      }
    }
    // } catch (error) {
    //   return response(res, error.message, {}, 500, false)
    // }
  },
  getNotif: async (req, res) => {
    try {
      const kode = req.user.kode
      const level = req.user.level
      const name = req.user.name
      const now = new Date(moment().format('YYYY-MM-DD'))
      const tomo = new Date(moment().format('YYYY-MM-DD 24:00:00'))
      console.log(kode)
      console.log()
      if (level === 4) {
        const result = await notif.findAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'sa' }
            ],
            reject: 'true'
          },
          order: [['id', 'DESC']],
          include: [
            {
              model: Path,
              as: 'dokumen'
            },
            {
              model: activity,
              as: 'active'
            }
          ]
        })
        if (result) {
          return response(res, 'success get notif', { result })
        } else {
          return response(res, 'failed get notif', {}, 404, false)
        }
      } else if (level === 5) {
        const result = await notif.findAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'kasir' }
            ],
            reject: 'true'
          },
          order: [['id', 'DESC']],
          include: [
            {
              model: Path,
              as: 'dokumen'
            },
            {
              model: activity,
              as: 'active'
            }
          ]
        })
        if (result) {
          return response(res, 'success get notif', { result })
        } else {
          return response(res, 'failed get notif', {}, 404, false)
        }
      } else if (level === 2) {
        const find = await pic.findAndCountAll({
          where: {
            spv: name
          },
          include: [
            {
              model: depo,
              as: 'depo'
            }
          ]
        })
        if (find) {
          const depos = []
          find.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await notif.findAndCountAll({
                where: {
                  [Op.and]: [
                    { kode_plant: depos[i].kode_depo },
                    { upload: 'true' }
                  ],
                  createdAt: {
                    [Op.lt]: tomo,
                    [Op.gt]: now
                  }
                },
                order: [['id', 'ASC']],
                limit: 10,
                include: [
                  {
                    model: Path,
                    as: 'dokumen'
                  },
                  {
                    model: activity,
                    as: 'active'
                  }
                ]
              })
              if (result.rows[0]) {
                result.rows.map(item => {
                  return sa.push(item)
                })
              }
            }
            if (sa.length > 0) {
              return response(res, 'list dokumen', { sa, kasir })
            } else {
              return response(res, 'list dokumen', { sa, kasir })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get notif', {}, 404, false)
        }
      } else if (level === 3) {
        const find = await pic.findAndCountAll({
          where: {
            pic: name
          },
          include: [
            {
              model: depo,
              as: 'depo'
            }
          ]
        })
        if (find) {
          const depos = []
          find.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await notif.findAndCountAll({
                where: {
                  [Op.and]: [
                    { kode_plant: depos[i].kode_depo },
                    { upload: 'true' }
                  ],
                  createdAt: {
                    [Op.lt]: tomo,
                    [Op.gt]: now
                  }
                },
                order: [['id', 'ASC']],
                limit: 10,
                include: [
                  {
                    model: Path,
                    as: 'dokumen'
                  },
                  {
                    model: activity,
                    as: 'active'
                  }
                ]
              })
              if (result.rows[0]) {
                result.rows.map(item => {
                  return sa.push(item)
                })
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { sa, kasir })
            } else {
              return response(res, 'list dokumen', { sa, kasir })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get notif', {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  // updateNotif: async (req, res) => {
  //   try {
  //     const id = req.params.id
  //     const level = req.user.level
  //     if (level === 2) {
  //       const result = await notif.findByPk(id)

  //     } else {
  //     }
  //   } catch (error) {
  //     return response(res, error.message, {}, 500, false)
  //   }
  // },
  getAllActivity: async (req, res) => {
    try {
      const level = req.user.level
      const name = req.user.name
      let { limit, page, search, time, tipe, find } = req.query
      let searchValue = ''
      let timeValue = ''
      let tipeValue = ''
      let findValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || ''
      }
      if (typeof find === 'object') {
        findValue = Object.values(find)[0]
      } else {
        findValue = find || ''
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
      let now = new Date(moment().clone().startOf('month').format('YYYY-MM-DD'))
      let tomo = new Date(moment().add(1, 'month').endOf('month').format('YYYY-MM-DD'))
      if (tipeValue === 'monthly') {
        now = new Date(moment().clone().startOf('year').format('YYYY-MM-DD'))
        tomo = new Date(moment().clone().endOf('year').format('YYYY-MM-DD'))
      }
      if (level === 2) {
        const results = await pic.findAndCountAll({
          where: {
            spv: name
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
            }
          ]
        })
        const pageInfo = pagination('/dashboard/active', req.query, page, limit, results.count)
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
                    limit: 31,
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
                    limit: 31,
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
      } else {
        return response(res, "you're not user spv", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  editAccessActive: async (req, res) => {
    try {
      const id = req.params.id
      const level = req.user.level
      const schema = joi.object({
        access: joi.string().valid('lock', 'unlock')
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 404, false)
      } else {
        if (level === 2) {
          const result = await activity.findByPk(id)
          if (result) {
            await result.update(results)
            return response(res, 'success update activity')
          } else {
            return response(res, 'failed update activity', {}, 404, false)
          }
        } else {
          return response(res, "you're not user spv", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
