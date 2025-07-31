'use strict'

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.changeColumn('depos', 'status_depo', {
      type: Sequelize.STRING
    })
    await queryInterface.changeColumn('documents', 'status_depo', {
      type: Sequelize.STRING
    })
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.changeColumn('depos', 'status_depo', {
      type: Sequelize.ENUM
    })
    await queryInterface.changeColumn('documents', 'status_depo', {
      type: Sequelize.ENUM
    })
  }
}
