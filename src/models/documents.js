'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class documents extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here]
    }
  };
  documents.init({
    nama_dokumen: DataTypes.STRING,
    jenis_dokumen: DataTypes.ENUM('daily', 'monthly'),
    postDokumen: DataTypes.DATE,
    divisi: DataTypes.STRING,
    status_depo: DataTypes.ENUM('Cabang SAP', 'Cabang Scylla', 'Depo SAP', 'Depo Scylla'),
    uploadedBy: DataTypes.ENUM('sa', 'kasir'),
    status: DataTypes.ENUM('active', 'inactive'),
    access: DataTypes.TEXT
  }, {
    sequelize,
    modelName: 'documents'
  })
  return documents
}
