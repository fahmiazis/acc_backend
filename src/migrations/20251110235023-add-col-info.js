'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.addColumn('report_invens', 'info', {
      type: Sequelize.DataTypes.TEXT
    })
  },

  down: async (queryInterface, Sequelize) => {
     await queryInterface.removeColumn('report_invens', 'info', {})
  }
};
