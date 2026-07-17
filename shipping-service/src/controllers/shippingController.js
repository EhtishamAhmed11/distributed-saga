const db = require('../db');

const createShipment = async (req, res) => {
  const { orderId, address } = req.body;

  if (!orderId || !address) {
    return res.status(400).json({ error: 'orderId and address are required' });
  }

  try {
    const result = await db.query(
      'INSERT INTO shipments (order_id, address, status) VALUES ($1, $2, $3) RETURNING *',
      [orderId, address, 'SHIPPED']
    );
    console.log(`Shipment created for order ${orderId} to address ${address}`);
    return res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error('Error creating shipment:', err);
    return res.status(500).json({ error: 'Failed to create shipment' });
  }
};

const cancelShipment = async (req, res) => {
  const { orderId } = req.body;

  if (!orderId) {
    return res.status(400).json({ error: 'orderId is required' });
  }

  try {
    const result = await db.query(
      "UPDATE shipments SET status = $1 WHERE order_id = $2 AND status = 'SHIPPED' RETURNING *",
      ['CANCELLED', orderId]
    );

    if (result.rows.length === 0) {
      // Idempotency: If no active shipment exists, succeed
      return res.json({ message: 'No active shipment to cancel' });
    }

    console.log(`Shipment cancelled for order ${orderId}`);
    return res.json(result.rows[0]);
  } catch (err) {
    console.error('Error cancelling shipment:', err);
    return res.status(500).json({ error: 'Failed to cancel shipment' });
  }
};

module.exports = {
  createShipment,
  cancelShipment
};
