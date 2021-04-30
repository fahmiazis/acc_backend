'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class date_clossing extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
    }
  };
  date_clossing.init({
    kode_depo: DataTypes.INTEGER,
    jenis: DataTypes.ENUM('daily', 'monthly'),
    day: DataTypes.DATEONLY,
    time: DataTypes.DATE
  }, {
    sequelize,
    modelName: 'date_clossing'
  })
  return date_clossing
}
