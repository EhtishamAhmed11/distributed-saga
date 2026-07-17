const { kafka } = require('./kafka');
const db = require('../db');
const { createOutboxEvent, trackEventId } = require('./outbox');

const consumer = kafka.consumer({ groupId: 'payment-service-group' });

const startPaymentConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'saga-commands', fromBeginning: true });
  console.log('Payment Service Kafka Consumer subscribed to "saga-commands"');

  await consumer.run({
    eachMessage: async ({ message }) => {
      let command;
      try {
        command = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('[Payment Service Kafka] Failed to parse message:', err);
        return;
      }

      if (!['CHARGE_PAYMENT', 'REFUND_PAYMENT'].includes(command.type)) {
        return;
      }

      console.log(`[Payment Service Kafka] Processing command: ${command.type} for Saga ${command.sagaId}`);

      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');

        // 1. Idempotency Check: insert event ID
        await trackEventId(client, command.event_id);

        switch (command.type) {
          case 'CHARGE_PAYMENT': {
            const { orderId, amount } = command;
            const idempotencyKey = `saga-${command.sagaId}`;

            // Check if key already exists
            const keyCheck = await client.query(
              'SELECT * FROM idempotency_keys WHERE key = $1 FOR UPDATE',
              [idempotencyKey]
            );

            if (keyCheck.rows.length > 0) {
              const saved = keyCheck.rows[0];
              if (saved.response_status === 201) {
                await createOutboxEvent(client, 'saga-events', 'PAYMENT_CHARGED', {
                  sagaId: command.sagaId,
                  orderId
                });
              } else {
                await createOutboxEvent(client, 'saga-events', 'PAYMENT_CHARGE_FAILED', {
                  sagaId: command.sagaId,
                  orderId,
                  error: saved.response_body.error || 'Payment declined (Cached)'
                });
              }
              break;
            }

            const chargeAmount = parseFloat(amount);
            let responseStatus = 201;
            let responseBody = {};

            if (chargeAmount === 999.99) {
              responseStatus = 400;
              responseBody = { error: 'Payment declined: Insufficient funds (Simulated)' };
              
              await createOutboxEvent(client, 'saga-events', 'PAYMENT_CHARGE_FAILED', {
                sagaId: command.sagaId,
                orderId,
                error: responseBody.error
              });
            } else {
              const paymentResult = await client.query(
                "INSERT INTO payments (order_id, amount, status) VALUES ($1, $2, 'COMPLETED') RETURNING *",
                [orderId, chargeAmount]
              );
              responseBody = paymentResult.rows[0];

              await createOutboxEvent(client, 'saga-events', 'PAYMENT_CHARGED', {
                sagaId: command.sagaId,
                orderId
              });
            }

            // Save Response to Idempotency Table
            await client.query(
              'INSERT INTO idempotency_keys (key, response_status, response_body) VALUES ($1, $2, $3)',
              [idempotencyKey, responseStatus, JSON.stringify(responseBody)]
            );
            break;
          }

          case 'REFUND_PAYMENT': {
            const { orderId } = command;

            // Mark completed payment as REFUNDED
            await client.query(
              "UPDATE payments SET status = 'REFUNDED' WHERE order_id = $1 AND status = 'COMPLETED'",
              [orderId]
            );

            // Queue success event
            await createOutboxEvent(client, 'saga-events', 'PAYMENT_REFUNDED', {
              sagaId: command.sagaId,
              orderId
            });
            break;
          }
        }

        await client.query('COMMIT');
        console.log(`[Payment Service Kafka] Command ${command.type} for Saga ${command.sagaId} committed.`);
      } catch (err) {
        await client.query('ROLLBACK');
        if (err.code === '23505') {
          console.log(`[Payment Service Kafka Idempotency] Duplicate event detected. Event "${command.event_id}" already processed.`);
        } else {
          console.error(`[Payment Service Kafka] Error executing transaction for command ${command.type}:`, err);
        }
      } finally {
        client.release();
      }
    }
  });
};

module.exports = { startPaymentConsumer };
