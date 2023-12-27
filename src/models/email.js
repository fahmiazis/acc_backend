'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class email extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
    }
  };
  email.init({
    kode_plant: DataTypes.STRING,
    area: DataTypes.STRING,
    email_sa_kasir: DataTypes.STRING,
    email_aos: DataTypes.STRING,
    email_ho_pic: DataTypes.STRING,
    email_bm: DataTypes.STRING,
    email_grom: DataTypes.STRING,
    email_rom: DataTypes.STRING,
    email_ho_1: DataTypes.STRING,
    email_ho_2: DataTypes.STRING,
    email_ho_3: DataTypes.STRING,
    email_ho_4: DataTypes.STRING,
    tipe: DataTypes.ENUM('sa', 'kasir'),
    status: DataTypes.ENUM('active', 'inactive')
  }, {
    sequelize,
    modelName: 'email'
  })
  return email
}
