const db = require('../db');

const chargePayment = async (req, res) => {
  const { orderId, amount } = req.body;
  const idempotencyKey = req.headers['idempotency-key'];

  if (!orderId || amount === undefined) {
    return res.status(400).json({ error: 'orderId and amount are required' });
  }

  // If idempotency-key header is provided, check/insert it atomically
  if (idempotencyKey) {
    const client = await db.pool.connect();
    try {
      await client.query('BEGIN');

      // 1. Lock and check if key already exists
      const keyCheck = await client.query(
        'SELECT * FROM idempotency_keys WHERE key = $1 FOR UPDATE',
        [idempotencyKey]
      );

      if (keyCheck.rows.length > 0) {
        // Key exists, return previous saved response
        await client.query('COMMIT');
        const saved = keyCheck.rows[0];
        console.log(`[Payment Idempotency] Duplicate request detected for key "${idempotencyKey}". Returning cached response.`);
        return res.status(saved.response_status).json(saved.response_body);
      }

      // 2. Execute Payment Business Logic
      const chargeAmount = parseFloat(amount);
      let responseStatus = 201;
      let responseBody = {};

      if (chargeAmount === 999.99) {
        console.warn(`Payment failed for order ${orderId}: Simulated charge failure for amount 999.99`);
        responseStatus = 400;
        responseBody = { error: 'Payment declined: Insufficient funds or card error (Simulated)' };
      } else {
        const result = await client.query(
          'INSERT INTO payments (order_id, amount, status) VALUES ($1, $2, $3) RETURNING *',
          [orderId, chargeAmount, 'COMPLETED']
        );
        responseBody = result.rows[0];
        console.log(`Payment of ${chargeAmount} charged for order ${orderId}`);
      }

      // 3. Save Response to Idempotency Table
      await client.query(
        'INSERT INTO idempotency_keys (key, response_status, response_body) VALUES ($1, $2, $3)',
        [idempotencyKey, responseStatus, JSON.stringify(responseBody)]
      );

      await client.query('COMMIT');
      return res.status(responseStatus).json(responseBody);
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('Error charging payment with idempotency:', err);
      return res.status(500).json({ error: 'Failed to charge payment' });
    } finally {
      client.release();
    }
  }

  // Fallback: If no idempotency key is provided, perform standard charge
  const chargeAmount = parseFloat(amount);
  if (chargeAmount === 999.99) {
    console.warn(`Payment failed for order ${orderId}: Simulated charge failure for amount 999.99`);
    return res.status(400).json({ error: 'Payment declined: Insufficient funds or card error (Simulated)' });
  }

  try {
    const result = await db.query(
      'INSERT INTO payments (order_id, amount, status) VALUES ($1, $2, $3) RETURNING *',
      [orderId, chargeAmount, 'COMPLETED']
    );
    console.log(`Payment of ${chargeAmount} charged for order ${orderId}`);
    return res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error('Error charging payment:', err);
    return res.status(500).json({ error: 'Failed to charge payment' });
  }
};

const refundPayment = async (req, res) => {
  const { orderId } = req.body;

  if (!orderId) {
    return res.status(400).json({ error: 'orderId is required' });
  }

  try {
    const result = await db.query(
      "UPDATE payments SET status = $1 WHERE order_id = $2 AND status = 'COMPLETED' RETURNING *",
      ['REFUNDED', orderId]
    );

    if (result.rows.length === 0) {
      // Idempotency: If no active payment was completed, succeed
      return res.json({ message: 'No active payment to refund' });
    }

    console.log(`Payment refunded for order ${orderId}`);
    return res.json(result.rows[0]);
  } catch (err) {
    console.error('Error refunding payment:', err);
    return res.status(500).json({ error: 'Failed to refund payment' });
  }
};

module.exports = {
  chargePayment,
  refundPayment
};
