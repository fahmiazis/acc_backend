'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class inventory extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      // define association here
    }
  };
  inventory.init({
    plant: DataTypes.STRING,
    area: DataTypes.STRING,
    channel: DataTypes.STRING,
    profit_center: DataTypes.STRING,
    kode_dist: DataTypes.STRING,
    pic_inv: DataTypes.STRING,
    pic_kasbank: DataTypes.STRING,
    status_area: DataTypes.STRING
  }, {
    sequelize,
    modelName: 'inventory',
  });
  return inventory;
};