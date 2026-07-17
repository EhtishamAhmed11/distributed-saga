const express = require('express');
const router = express.Router();
const shippingController = require('../controllers/shippingController');

router.post('/ship', shippingController.createShipment);
router.post('/cancel', shippingController.cancelShipment);

module.exports = router;
