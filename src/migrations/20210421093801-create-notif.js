'use strict'
module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('notifs', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      kode_plant: {
        type: Sequelize.STRING
      },
      reject: {
        type: Sequelize.ENUM('true', 'false')
      },
      upload: {
        type: Sequelize.ENUM('true', 'false')
      },
      activityId: {
        type: Sequelize.INTEGER
      },
      pathId: {
        type: Sequelize.INTEGER
      },
      tipe: {
        type: Sequelize.ENUM('kasir', 'sa')
      },
      createdAt: {
        allowNull: false,
        type: Sequelize.DATE
      },
      updatedAt: {
        allowNull: false,
        type: Sequelize.DATE
      }
    })
  },
  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('notifs')
  }
}
