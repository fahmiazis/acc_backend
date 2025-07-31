'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class depo extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
      depo.hasMany(models.documents, {
        foreignKey: 'status_depo',
        as: 'dokumen',
        sourceKey: 'status_depo'
      })

      depo.hasMany(models.users, {
        sourceKey: 'kode_plant',
        foreignKey: 'kode_depo',
        as: 'area'
      })
      depo.hasMany(models.activity, {
        sourceKey: 'kode_plant',
        foreignKey: 'kode_plant',
        as: 'active'
      })
      depo.hasMany(models.email, {
        sourceKey: 'kode_plant',
        foreignKey: 'kode_plant',
        as: 'emails'
      })
    }
  };
  depo.init({
    kode_depo: DataTypes.STRING,
    nama_depo: DataTypes.STRING,
    home_town: DataTypes.STRING,
    channel: DataTypes.STRING,
    distribution: DataTypes.STRING,
    // status_depo: DataTypes.ENUM('Cabang SAP', 'Cabang Scylla', 'Depo SAP', 'Depo Scylla'),
    status_depo: DataTypes.STRING,
    profit_center: DataTypes.STRING,
    kode_plant: DataTypes.STRING,
    kode_sap_1: DataTypes.INTEGER,
    kode_sap_2: DataTypes.INTEGER,
    nama_grom: DataTypes.STRING,
    nama_bm: DataTypes.STRING,
    nama_ass: DataTypes.STRING,
    nama_pic_1: DataTypes.STRING,
    nama_pic_2: DataTypes.STRING,
    nama_pic_3: DataTypes.STRING,
    nama_pic_4: DataTypes.STRING
  }, {
    sequelize,
    modelName: 'depo'
  })
  return depo
}
