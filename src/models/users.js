'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class users extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
      users.hasOne(models.depo, {
        sourceKey: 'kode_depo',
        foreignKey: 'kode_plant',
        as: 'depo'
      })
    }
  };
  users.init({
    username: DataTypes.STRING,
    password: DataTypes.STRING,
    kode_depo: DataTypes.STRING,
    nama_depo: DataTypes.STRING,
    user_level: DataTypes.INTEGER,
    status: DataTypes.ENUM('active', 'inactive')
  }, {
    sequelize,
    modelName: 'users'
  })
  return users
}
