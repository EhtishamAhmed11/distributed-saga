const db = require('../db');

const createOrder = async (req, res) => {
  const { productId, quantity, totalPrice } = req.body;
  
  if (!productId || !quantity || totalPrice === undefined) {
    return res.status(400).json({ error: 'productId, quantity, and totalPrice are required' });
  }

  try {
    const result = await db.query(
      'INSERT INTO orders (product_id, quantity, total_price, status) VALUES ($1, $2, $3, $4) RETURNING *',
      [productId, quantity, totalPrice, 'PENDING']
    );
    return res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error('Error creating order:', err);
    return res.status(500).json({ error: 'Failed to create order' });
  }
};

const updateOrderStatus = async (req, res) => {
  const { id } = req.params;
  const { status } = req.body;

  if (!status) {
    return res.status(400).json({ error: 'status is required' });
  }

  try {
    const result = await db.query(
      'UPDATE orders SET status = $1 WHERE id = $2 RETURNING *',
      [status, id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    return res.json(result.rows[0]);
  } catch (err) {
    console.error('Error updating order status:', err);
    return res.status(500).json({ error: 'Failed to update order status' });
  }
};

module.exports = {
  createOrder,
  updateOrderStatus
};
