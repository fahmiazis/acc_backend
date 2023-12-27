'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class activity extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
      activity.hasMany(models.Path, {
        sourceKey: 'id',
        foreignKey: 'activityId',
        as: 'doc'
      })
    }
  };
  activity.init({
    kode_plant: DataTypes.STRING,
    progress: DataTypes.INTEGER,
    documentDate: DataTypes.DATE,
    status: DataTypes.STRING,
    access: DataTypes.ENUM('lock', 'unlock'),
    jenis_dokumen: DataTypes.ENUM('daily', 'monthly'),
    tipe: DataTypes.ENUM('sa', 'kasir')
  }, {
    sequelize,
    modelName: 'activity'
  })
  return activity
}
