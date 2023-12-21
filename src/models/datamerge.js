'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class datamerge extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
    }
  }
  datamerge.init({
    kode_depo: DataTypes.STRING,
    nama_depo: DataTypes.STRING,
    kode_outlet: DataTypes.STRING,
    nama_outlet: DataTypes.STRING,
    kode_sales: DataTypes.STRING,
    nama_sales: DataTypes.STRING,
    tgl_faktur: DataTypes.DATE,
    no_faktur: DataTypes.STRING,
    gross_sales: DataTypes.STRING,
    rp_discpc: DataTypes.STRING,
    disc1: DataTypes.STRING,
    disc2: DataTypes.STRING,
    pro_amount: DataTypes.STRING,
    cash_disct: DataTypes.STRING,
    ppn: DataTypes.STRING,
    total: DataTypes.STRING,
    type: DataTypes.STRING,
    pcode: DataTypes.STRING,
    nama_produk: DataTypes.STRING,
    qty_pcs: DataTypes.STRING,
    kode_retur: DataTypes.STRING,
    nama_retur: DataTypes.STRING,
    tgl_retur: DataTypes.DATE,
    invort: DataTypes.STRING,
    remark: DataTypes.STRING,
    keterangan: DataTypes.STRING
  }, {
    sequelize,
    modelName: 'datamerge'
  })
  return datamerge
}
