'use strict';
const {
  Model
} = require('sequelize');
module.exports = (sequelize, DataTypes) => {
  class movement extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate(models) {
      // define association here
    }
  };
  movement.init({
    mv_type: DataTypes.STRING,
    mv_text: DataTypes.STRING,
    mv_grouping: DataTypes.STRING,
    comp_grouping: DataTypes.STRING,
    storage_loc: DataTypes.STRING,
    saldo: DataTypes.STRING,
    status: DataTypes.INTEGER
  }, {
    sequelize,
    modelName: 'movement',
  });
  return movement;
};