const db = require('../db');

const reserveInventory = async (req, res) => {
  const { orderId, productId, quantity } = req.body;

  if (!orderId || !productId || !quantity) {
    return res.status(400).json({ error: 'orderId, productId, and quantity are required' });
  }

  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');

    const existingResult = await client.query(
      'SELECT * FROM reservations WHERE order_id = $1 FOR UPDATE',
      [orderId]
    );

    if (existingResult.rows.length > 0) {
      const existing = existingResult.rows[0];
      if (existing.status === 'ACTIVE' || existing.status === 'RESERVED') {
        await client.query('COMMIT');
        console.log(`[Inventory Idempotency] Duplicate reserve request for order ${orderId}.`);
        return res.status(200).json(existing);
      } else if (existing.status === 'RELEASED') {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'Reservation for this order has already been released' });
      }
    }

    const productResult = await client.query(
      'SELECT stock FROM products WHERE id = $1 FOR UPDATE',
      [productId]
    );

    if (productResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Product not found' });
    }

    const currentStock = productResult.rows[0].stock;
    if (currentStock < quantity) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Insufficient stock' });
    }

    await client.query(
      'UPDATE products SET stock = stock - $1 WHERE id = $2',
      [quantity, productId]
    );

    const reservationResult = await client.query(
      "INSERT INTO reservations (order_id, product_id, quantity, status) VALUES ($1, $2, $3, 'ACTIVE') RETURNING *",
      [orderId, productId, quantity]
    );

    await client.query('COMMIT');
    console.log(`Inventory reserved for order ${orderId}, product ${productId}`);
    return res.status(201).json(reservationResult.rows[0]);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error reserving inventory:', err);
    return res.status(500).json({ error: 'Failed to reserve inventory' });
  } finally {
    client.release();
  }
};

const releaseInventory = async (req, res) => {
  const { orderId } = req.body;

  if (!orderId) {
    return res.status(400).json({ error: 'orderId is required' });
  }

  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');

    const reservationResult = await client.query(
      "SELECT * FROM reservations WHERE order_id = $1 FOR UPDATE",
      [orderId]
    );

    if (reservationResult.rows.length === 0) {
      await client.query('COMMIT');
      return res.json({ message: 'No reservation exists to release' });
    }

    const reservation = reservationResult.rows[0];

    if (reservation.status === 'RELEASED') {
      await client.query('COMMIT');
      console.log(`[Inventory Idempotency] Duplicate release request for order ${orderId}.`);
      return res.json({ message: 'Reservation has already been released' });
    }

    await client.query(
      "UPDATE reservations SET status = 'RELEASED' WHERE id = $1",
      [reservation.id]
    );

    await client.query(
      'UPDATE products SET stock = stock + $1 WHERE id = $2',
      [reservation.quantity, reservation.product_id]
    );

    await client.query('COMMIT');
    console.log(`Inventory released for order ${orderId}, product ${reservation.product_id}`);
    return res.json({ message: 'Inventory released successfully' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error releasing inventory:', err);
    return res.status(500).json({ error: 'Failed to release inventory' });
  } finally {
    client.release();
  }
};

module.exports = {
  reserveInventory,
  releaseInventory
};
