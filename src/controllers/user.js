const joi = require('joi')
const response = require('../helpers/response')
const { users, sequelize, pic } = require('../models')
const bcrypt = require('bcryptjs')
const { Op, QueryTypes } = require('sequelize')
const { pagination } = require('../helpers/pagination')
const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const uploadMaster = require('../helpers/uploadMaster')
const fs = require('fs')
const excel = require('exceljs')
const vs = require('fs-extra')
const { APP_URL } = process.env

module.exports = {
addUser: async (req, res) => {
    try {
      const level = req.user.level
      const schema = joi.object({
        username: joi.string().required(),
        password: joi.string().required(),
        kode_depo: joi.string().allow(''),
        nama_depo: joi.string().allow(''),
        user_level: joi.number().required(),
        status: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          const result = await users.findAll({ where: { username: results.username } })
          if (result.length > 0) {
            return response(res, 'username already use', {}, 404, false)
          } else {
            if (results.user_level === '5') {
              const result = await users.findAll({
                where: {
                  [Op.and]: [
                    { kode_depo: results.kode_depo },
                    { user_level: results.user_level }
                  ]
                }
              })
              if (result.length > 0) {
                return response(res, 'kode depo and user level already use', {}, 404, false)
              } else {
                results.password = await bcrypt.hash(results.password, await bcrypt.genSalt())
                const result = await users.create(results)
                if (result) {
                  return response(res, 'Add User succesfully', { result })
                } else {
                  return response(res, 'Fail to create user', {}, 400, false)
                }
              }
            } else {
              const result = await users.findAll({
                where: {
                  [Op.and]: [
                    { username: results.username }
                  ]
                }
              })
              if (result.length > 0) {
                return response(res, 'username already use', {}, 404, false)
              } else {
                results.password = await bcrypt.hash(results.password, await bcrypt.genSalt())
                const result = await users.create(results)
                if (result) {
                  return response(res, 'Add User succesfully', { result })
                } else {
                  return response(res, 'Fail to create user', {}, 400, false)
                }
              }
            }
          }
        } else {
          return response(res, "You're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  updateUser: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      const schema = joi.object({
        username: joi.string(),
        password: joi.string().allow(''),
        kode_depo: joi.string().allow(''),
        nama_depo: joi.string().allow(''),
        user_level: joi.number(),
        status: joi.string()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          if (results.kode_depo) {
            const result = await users.findAll({
              where: {
                [Op.and]: [
                  { kode_depo: results.kode_depo },
                  { user_level: results.user_level }
                ],
                [Op.not]: { id: id }
              }
            })
            if (result.length > 0) {
              return response(res, 'kode depo and user level already use', {}, 404, false)
            } else {
              if (results.username) {
                const result = await users.findAll({
                  where: {
                    username: results.username,
                    [Op.not]: { id: id }
                  }
                })
                if (result.length > 0) {
                  return response(res, 'username already use', { result }, 404, false)
                } else {
                  if (results.password !== '') {
                    results.password = await bcrypt.hash(results.password, await bcrypt.genSalt())
                    const result = await users.findByPk(id)
                    if (result) {
                      await result.update(results)
                      return response(res, 'update User succesfully', { result })
                    } else {
                      return response(res, 'Fail to update user', {}, 400, false)
                    }
                  } else {
                    const result = await users.findByPk(id)
                    if (result) {
                      await result.update(results)
                      return response(res, 'update User succesfully', { result })
                    } else {
                      return response(res, 'Fail to update user', {}, 400, false)
                    }
                  }
                }
              }
            }
          } else {
            if (results.username) {
              const result = await users.findAll({
                where: {
                  username: results.username,
                  [Op.not]: { id: id }
                }
              })
              if (result.length > 0) {
                return response(res, 'username already exist', { result }, 404, false)
              } else {
                if (results.password !== '') {
                  results.password = await bcrypt.hash(results.password, await bcrypt.genSalt())
                  const result = await users.findByPk(id)
                  if (result) {
                    await result.update(results)
                    return response(res, 'update User succesfully', { result })
                  } else {
                    return response(res, 'Fail to update user', {}, 400, false)
                  }
                } else {
                  const result = await users.findByPk(id)
                  if (result) {
                    await result.update(results)
                    return response(res, 'update User succesfully', { result })
                  } else {
                    return response(res, 'Fail to update user', {}, 400, false)
                  }
                }
              }
            }
          }
        } else {
          return response(res, "You're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deleteUser: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      if (level === 1) {
        const result = await users.findByPk(id)
        if (result) {
          await result.destroy()
          return response(res, 'delete user success', { result })
        } else {
          return response(res, 'user not found', {}, 404, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getUsers: async (req, res) => {
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
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const result = await users.findAndCountAll({
        where: {
          [Op.or]: [
            { username: { [Op.like]: `%${searchValue}%` } },
            { kode_depo: { [Op.like]: `%${searchValue}%` } },
            { nama_depo: { [Op.like]: `%${searchValue}%` } }
          ],
          [Op.not]: { user_level: 1 }
        },
        order: [[sortValue, 'ASC']],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/user/get', req.query, page, limit, result.count)
      if (result) {
        return response(res, 'list users', { result, pageInfo })
      } else {
        return response(res, 'failed to get user', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailUser: async (req, res) => {
    try {
      const id = req.params.id
      const result = await users.findByPk(id)
      if (result) {
        return response(res, `Profile of user with id ${id}`, { result })
      } else {
        return response(res, 'fail to get user', {}, 400, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadMasterUser: async (req, res) => {
    try {
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
            const cek = ['User Name', 'Password', 'Kode Depo', 'Nama Depo', 'User Level']
            const valid = rows[0]
            for (let i = 0; i < cek.length; i++) {
              if (valid[i] === cek[i]) {
                count.push(1)
              }
            }
            if (count.length === cek.length) {
              const plant = []
              const user = []
              const cek = []
              for (let i = 1; i < rows.length; i++) {
                const a = rows[i]
                if (a[2] !== '' || a[2] !== null) {
                  console.log('aabb')
                } else {
                  plant.push(`Kode depo ${a[2]} dan  User level ${a[4]}`)
                }
                user.push(`User Name ${a[0]}`)
                cek.push(`${a[0]}`)
              }
              const object = {}
              const result = []
              const obj = {}

              user.forEach(item => {
                if (!obj[item]) { obj[item] = 0 }
                obj[item] += 1
              })

              for (const prop in obj) {
                if (obj[prop] >= 2) {
                  result.push(prop)
                }
              }

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
                  const select = await sequelize.query(`SELECT username from users WHERE username='${cek[i]}'`, {
                    type: QueryTypes.SELECT
                  })
                  await sequelize.query(`DELETE from users WHERE username='${cek[i]}'`, {
                    type: QueryTypes.DELETE
                  })
                  if (select.length > 0) {
                    arr.push(select[0])
                  }
                }
                if (arr.length > 0) {
                  rows.shift()
                  const create = []
                  for (let i = 0; i < rows.length; i++) {
                    const noun = []
                    const process = rows[i]
                    for (let j = 0; j < process.length; j++) {
                      if (j === 1) {
                        let str = process[j]
                        str = await bcrypt.hashSync(str, await bcrypt.genSaltSync())
                        noun.push(str)
                      } else if (j === 4) {
                        noun.push(parseInt(process[j]))
                      } else {
                        noun.push(process[j])
                      }
                    }
                    create.push(noun)
                  }
                  const result = await sequelize.query(`INSERT INTO users (username, password, kode_depo, nama_depo, user_level) VALUES ${create.map(a => '(?)').join(',')}`,
                    {
                      replacements: create,
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
                  const create = []
                  for (let i = 0; i < rows.length; i++) {
                    const noun = []
                    const process = rows[i]
                    for (let j = 0; j < process.length; j++) {
                      if (j === 1) {
                        let str = process[j]
                        str = await bcrypt.hashSync(str, await bcrypt.genSaltSync())
                        noun.push(str)
                      } else if (j === 4) {
                        noun.push(parseInt(process[j]))
                      } else {
                        noun.push(process[j])
                      }
                    }
                    create.push(noun)
                  }
                  const result = await sequelize.query(`INSERT INTO users (username, password, kode_depo, nama_depo, user_level) VALUES ${create.map(a => '(?)').join(',')}`,
                    {
                      replacements: create,
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
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  exportSqlUser: async (req, res) => {
    try {
      const result = await users.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        const arr = []
        const header = ['User Name', 'Password', 'Kode Depo', 'Nama Depo', 'User Level']
        const key = ['username', 'password', 'kode_depo', 'nama_depo', 'user_level']
        for (let i = 0; i < header.length; i++) {
          let temp = { header: header[i], key: key[i] }
          arr.push(temp)
          temp = {}
        }
        worksheet.columns = arr
        const cek = worksheet.addRows(result)
        if (cek) {
          const name = new Date().getTime().toString().concat('-user').concat('.xlsx')
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
  createUserPic: async (req, res) => {
    try {
      const level = req.user.level
      if (level === 1) {
        const result = await pic.findAll()
        const user = await users.findAll()
        if (result) {
          const data = []
          const dataUser = []
          result.map(x => {
            return (
              data.push(x.pic)
            )
          })
          user.map(x => {
            return (
              dataUser.push(x.username)
            )
          })
          const set = new Set(data)
          const newData = [...set]
          const filter = []
          for (let i = 0; i < newData.length; i++) {
            const pos = dataUser.indexOf(newData[i])
            if (pos === -1) {
              filter.push(newData[i])
            }
          }
          console.log(filter)
          if (filter.length !== 0) {
            const send = []
            for (let i = 0; i < filter.length; i++) {
              const create = [filter[i], await bcrypt.hash(filter[i], await bcrypt.genSalt()), 3]
              send.push(create)
            }
            const results = await sequelize.query(`INSERT INTO users (username, password, user_level) VALUES ${send.map(a => '(?)').join(',')}`,
              {
                replacements: send,
                type: QueryTypes.INSERT
              })
            if (results) {
              return response(res, 'success create user pic')
            } else {
              return response(res, 'failed create user pic', {}, 404, false)
            }
          } else {
            return response(res, 'All Pic has user account')
          }
        } else {
          return response(res, 'failed get pic', {}, 404, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  createUserSpv: async (req, res) => {
    try {
      const level = req.user.level
      if (level === 1) {
        const result = await pic.findAll()
        const user = await users.findAll()
        if (result) {
          const data = []
          const dataUser = []
          result.map(x => {
            return (
              data.push(x.spv)
            )
          })
          user.map(x => {
            return (
              dataUser.push(x.username)
            )
          })
          const set = new Set(data)
          const newData = [...set]
          const filter = []
          for (let i = 0; i < newData.length; i++) {
            const pos = dataUser.indexOf(newData[i])
            if (pos === -1) {
              filter.push(newData[i])
            }
          }
          console.log(filter)
          if (filter.length !== 0) {
            const send = []
            for (let i = 0; i < filter.length; i++) {
              const create = [filter[i], await bcrypt.hash(filter[i], await bcrypt.genSalt()), 2]
              send.push(create)
            }
            const results = await sequelize.query(`INSERT INTO users (username, password, user_level) VALUES ${send.map(a => '(?)').join(',')}`,
              {
                replacements: send,
                type: QueryTypes.INSERT
              })
            if (results) {
              return response(res, 'success create user pic', { send })
            } else {
              return response(res, 'failed create user pic', {}, 404, false)
            }
          } else {
            return response(res, 'All SPV has user account')
          }
        } else {
          return response(res, 'failed get pic', {}, 404, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
