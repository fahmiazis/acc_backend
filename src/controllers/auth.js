const joi = require('joi')
const jwt = require('jsonwebtoken')
const bcrypt = require('bcryptjs')
const response = require('../helpers/response')
const { users } = require('../models')

const { APP_KEY } = process.env

module.exports = {
  login: async (req, res) => {
    try {
      const schema = joi.object({
        username: joi.string().required(),
        password: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        const result = await users.findOne({ where: { username: results.username } })
        if (result) {
          const { id, kode_depo, user_level, username } = result
          bcrypt.compare(results.password, result.password, function (_err, result) {
            if (result) {
              jwt.sign({ id: id, level: user_level, kode: kode_depo, name: username }, `${APP_KEY}`, (_err, token) => {
                return response(res, 'login success', { user: { id, kode_depo, user_level, username }, Token: `${token}` })
              })
            } else {
              return response(res, 'Wrong password', {}, 400, false)
            }
          })
        } else {
          return response(res, 'username is not registered', {}, 400, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  register: async (req, res) => {
    try {
      const schema = joi.object({
        username: joi.string().required(),
        password: joi.string().required(),
        kode_depo: joi.number(),
        nama_depo: joi.string(),
        user_level: joi.number().required(),
        status: joi.string().required()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        const result = await users.findOne({ where: { username: results.username } })
        if (result) {
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
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
