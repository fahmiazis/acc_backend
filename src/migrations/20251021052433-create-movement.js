'use strict';
module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('movements', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      mv_type: {
        type: Sequelize.STRING
      },
      mv_text: {
        type: Sequelize.STRING
      },
      mv_grouping: {
        type: Sequelize.STRING
      },
      comp_grouping: {
        type: Sequelize.STRING
      },
      storage_loc: {
        type: Sequelize.STRING
      },
      saldo: {
        type: Sequelize.STRING
      },
      status: {
        type: Sequelize.INTEGER
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
    await queryInterface.dropTable('movements');
  }
};