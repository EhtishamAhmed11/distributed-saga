const express = require('express');
const router = express.Router();
const inventoryController = require('../controllers/inventoryController');

router.post('/reserve', inventoryController.reserveInventory);
router.post('/release', inventoryController.releaseInventory);

module.exports = router;
