'use strict'
/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up (queryInterface, Sequelize) {
    await queryInterface.createTable('datamerges', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      kode_depo: {
        type: Sequelize.STRING
      },
      nama_depo: {
        type: Sequelize.STRING
      },
      kode_outlet: {
        type: Sequelize.STRING
      },
      nama_outlet: {
        type: Sequelize.STRING
      },
      kode_sales: {
        type: Sequelize.STRING
      },
      nama_sales: {
        type: Sequelize.STRING
      },
      tgl_faktur: {
        type: Sequelize.DATE
      },
      no_faktur: {
        type: Sequelize.STRING
      },
      gross_sales: {
        type: Sequelize.STRING
      },
      rp_discpc: {
        type: Sequelize.STRING
      },
      disc1: {
        type: Sequelize.STRING
      },
      disc2: {
        type: Sequelize.STRING
      },
      pro_amount: {
        type: Sequelize.STRING
      },
      cash_disct: {
        type: Sequelize.STRING
      },
      ppn: {
        type: Sequelize.STRING
      },
      total: {
        type: Sequelize.STRING
      },
      type: {
        type: Sequelize.STRING
      },
      pcode: {
        type: Sequelize.STRING
      },
      nama_produk: {
        type: Sequelize.STRING
      },
      qty_pcs: {
        type: Sequelize.STRING
      },
      kode_retur: {
        type: Sequelize.STRING
      },
      nama_retur: {
        type: Sequelize.STRING
      },
      tgl_retur: {
        type: Sequelize.DATE
      },
      invort: {
        type: Sequelize.STRING
      },
      remark: {
        type: Sequelize.STRING
      },
      keterangan: {
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
    })
  },
  async down (queryInterface, Sequelize) {
    await queryInterface.dropTable('datamerges')
  }
}
