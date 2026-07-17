const { kafka } = require('./kafka');
const db = require('../db');
const { createOutboxEvent, trackEventId } = require('./outbox');

const consumer = kafka.consumer({ groupId: 'order-service-group' });

const startOrderConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'saga-commands', fromBeginning: true });
  console.log('Order Service Kafka Consumer subscribed to "saga-commands"');

  await consumer.run({
    eachMessage: async ({ message }) => {
      let command;
      try {
        command = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('[Order Service Kafka] Failed to parse message value:', err);
        return;
      }

      if (!['CREATE_ORDER', 'CANCEL_ORDER', 'CONFIRM_ORDER'].includes(command.type)) {
        return;
      }

      console.log(`[Order Service Kafka] Processing command: ${command.type} for Saga ${command.sagaId}`);

      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');

        // 1. Idempotency Check: insert event ID
        await trackEventId(client, command.event_id);

        switch (command.type) {
          case 'CREATE_ORDER': {
            // Business logic: Insert order
            const orderResult = await client.query(
              "INSERT INTO orders (product_id, quantity, total_price, status) VALUES ($1, $2, $3, 'PENDING') RETURNING id",
              [command.productId, command.quantity, command.totalPrice]
            );
            const orderId = orderResult.rows[0].id;

            // Queue output event in Outbox
            await createOutboxEvent(client, 'saga-events', 'ORDER_CREATED', {
              sagaId: command.sagaId,
              orderId,
              productId: command.productId,
              quantity: command.quantity,
              totalPrice: command.totalPrice
            });
            break;
          }

          case 'CANCEL_ORDER': {
            // Business logic: Cancel order
            const cancelResult = await client.query(
              "UPDATE orders SET status = 'CANCELLED' WHERE id = $1 RETURNING id",
              [command.orderId]
            );

            if (cancelResult.rows.length === 0) {
              await createOutboxEvent(client, 'saga-events', 'ORDER_CANCEL_FAILED', {
                sagaId: command.sagaId,
                orderId: command.orderId,
                error: 'Order not found'
              });
            } else {
              // Queue output event in Outbox
              await createOutboxEvent(client, 'saga-events', 'ORDER_CANCELLED', {
                sagaId: command.sagaId,
                orderId: command.orderId
              });
            }
            break;
          }

          case 'CONFIRM_ORDER': {
            // Business logic: Confirm order
            const confirmResult = await client.query(
              "UPDATE orders SET status = 'CONFIRMED' WHERE id = $1 RETURNING id",
              [command.orderId]
            );

            if (confirmResult.rows.length === 0) {
              await createOutboxEvent(client, 'saga-events', 'ORDER_CONFIRM_FAILED', {
                sagaId: command.sagaId,
                orderId: command.orderId,
                error: 'Order not found'
              });
            } else {
              // Queue output event in Outbox
              await createOutboxEvent(client, 'saga-events', 'ORDER_CONFIRMED', {
                sagaId: command.sagaId,
                orderId: command.orderId
              });
            }
            break;
          }
        }

        await client.query('COMMIT');
        console.log(`[Order Service Kafka] Command ${command.type} for Saga ${command.sagaId} committed.`);
      } catch (err) {
        await client.query('ROLLBACK');
        if (err.code === '23505') {
          console.log(`[Order Service Kafka Idempotency] Duplicate event detected. Event "${command.event_id}" already processed.`);
        } else {
          console.error(`[Order Service Kafka] Error executing transaction for command ${command.type}:`, err);
        }
      } finally {
        client.release();
      }
    }
  });
};

module.exports = { startOrderConsumer };
