const express = require('express');
const router = express.Router();
const sagaController = require('../controllers/sagaController');

router.post('/order', sagaController.startSaga);

module.exports = router;
