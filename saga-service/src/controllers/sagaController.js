const sagaService = require('../services/sagaService');

const startSaga = async (req, res) => {
  const { productId, quantity, totalPrice, address } = req.body;

  if (!productId || !quantity || totalPrice === undefined || !address) {
    return res.status(400).json({ error: 'productId, quantity, totalPrice, and address are required' });
  }

  try {
    const sagaId = await sagaService.startSaga({ productId, quantity, totalPrice, address });
    
    // Return 202 Accepted immediately
    return res.status(202).json({
      message: 'Order request accepted, processing via Saga',
      sagaId
    });
  } catch (err) {
    console.error('Error starting saga:', err);
    res.status(500).json({ error: err.message });
  }
};

module.exports = {
  startSaga
};
