const db = require('../db');
const { createOutboxEvent } = require('../utils/outbox');

/**
 * Initiates the asynchronous distributed saga transaction using Transactional Outbox
 * @param {Object} orderData 
 * @returns {Promise<number>} the created sagaId
 */
const startSaga = async (orderData) => {
  const { productId, quantity, totalPrice, address } = orderData;

  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Write the initial Saga state to Database
    const result = await client.query(
      `INSERT INTO sagas (current_step, status, product_id, quantity, total_price, address) 
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
      ['STARTED', 'PROCESSING', productId, quantity, totalPrice, address]
    );
    const sagaId = result.rows[0].id;

    // 2. Queue CREATE_ORDER Command in Outbox table
    console.log(`[Saga Orchestrator] [Saga ${sagaId}] Queueing CREATE_ORDER command in Outbox table`);
    await createOutboxEvent(client, 'saga-commands', 'CREATE_ORDER', {
      sagaId,
      productId,
      quantity,
      totalPrice
    });

    await client.query('COMMIT');
    console.log(`[Saga Orchestrator] Created Saga Instance ID: ${sagaId} in Database & Transaction Committed`);
    return sagaId;
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[Saga Orchestrator] Failed to start saga transaction:', err);
    throw err;
  } finally {
    client.release();
  }
};

module.exports = {
  startSaga
};
