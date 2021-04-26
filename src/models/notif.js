'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class notif extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
      notif.hasOne(models.Path, {
        as: 'dokumen',
        sourceKey: 'pathId',
        foreignKey: 'id'
      })
      notif.hasOne(models.activity, {
        as: 'active',
        sourceKey: 'activityId',
        foreignKey: 'id'
      })
    }
  };
  notif.init({
    kode_plant: DataTypes.STRING,
    reject: DataTypes.ENUM('true', 'false'),
    upload: DataTypes.ENUM('true', 'false'),
    activityId: DataTypes.INTEGER,
    pathId: DataTypes.INTEGER,
    tipe: DataTypes.ENUM('kasir', 'sa')
  }, {
    sequelize,
    modelName: 'notif'
  })
  return notif
}
