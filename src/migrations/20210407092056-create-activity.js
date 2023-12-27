'use strict'
module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('activities', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      kode_plant: {
        type: Sequelize.STRING
      },
      progress: {
        type: Sequelize.INTEGER,
        defaultValue: 0
      },
      documentDate: {
        type: Sequelize.DATE
      },
      status: {
        type: Sequelize.STRING
      },
      access: {
        type: Sequelize.ENUM('lock', 'unlock'),
        defaultValue: 'unlock'
      },
      jenis_dokumen: {
        type: Sequelize.ENUM('daily', 'monthly')
      },
      tipe: {
        type: Sequelize.ENUM('sa', 'kasir')
      },
      createdAt: {
        allowNull: false,
        type: Sequelize.DATE,
        defaultValue: Sequelize.fn('NOW')
      },
      updatedAt: {
        allowNull: false,
        type: Sequelize.DATE,
        defaultValue: Sequelize.fn('NOW')
      }
    })
  },
  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('activities')
  }
}
