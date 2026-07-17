const express = require('express');
const router = express.Router();
const paymentController = require('../controllers/paymentController');

router.post('/charge', paymentController.chargePayment);
router.post('/refund', paymentController.refundPayment);

module.exports = router;
