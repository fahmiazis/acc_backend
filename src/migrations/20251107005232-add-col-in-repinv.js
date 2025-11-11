'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.addColumn('report_invens', 'plant', {
      type: Sequelize.DataTypes.STRING
    })
    await queryInterface.addColumn('report_invens', 'date_report', {
      type: Sequelize.DataTypes.DATE
    })
    await queryInterface.addColumn('report_invens', 'user_upload', {
      type: Sequelize.DataTypes.STRING
    })
    await queryInterface.addColumn('report_invens', 'status_report', {
      type: Sequelize.DataTypes.STRING
    })
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.removeColumn('report_invens', 'plant', {})
    await queryInterface.removeColumn('report_invens', 'date_report', {})
    await queryInterface.removeColumn('report_invens', 'user_upload', {})
    await queryInterface.removeColumn('report_invens', 'status_report', {})
  }
};
