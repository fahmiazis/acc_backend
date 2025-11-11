'use strict';
module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('inventories', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      plant: {
        type: Sequelize.STRING
      },
      area: {
        type: Sequelize.STRING
      },
      channel: {
        type: Sequelize.STRING
      },
      profit_center: {
        type: Sequelize.STRING
      },
      kode_dist: {
        type: Sequelize.STRING
      },
      pic_inv: {
        type: Sequelize.STRING
      },
      pic_kasbank: {
        type: Sequelize.STRING
      },
      status_area: {
        type: Sequelize.STRING
      },
      createdAt: {
        allowNull: false,
        type: Sequelize.DATE
      },
      updatedAt: {
        allowNull: false,
        type: Sequelize.DATE
      }
    });
  },
  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('inventories');
  }
};