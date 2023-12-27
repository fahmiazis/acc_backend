'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class pic extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
      pic.hasOne(models.depo, {
        sourceKey: 'kode_depo',
        foreignKey: 'kode_plant',
        as: 'depo'
      })

      pic.hasMany(models.users, {
        sourceKey: 'kode_depo',
        foreignKey: 'kode_depo',
        as: 'area'
      })
    }
  };
  pic.init({
    pic: DataTypes.STRING,
    spv: DataTypes.STRING,
    divisi: DataTypes.STRING,
    kode_depo: DataTypes.INTEGER,
    nama_depo: DataTypes.STRING,
    status: DataTypes.ENUM('active', 'inactive')
  }, {
    sequelize,
    modelName: 'pic'
  })
  return pic
}
