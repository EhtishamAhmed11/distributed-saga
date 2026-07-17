const { kafka } = require('./kafka');
const db = require('../db');
const { createOutboxEvent, trackEventId } = require('./outbox');

const consumer = kafka.consumer({ groupId: 'shipping-service-group' });

const startShippingConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'saga-commands', fromBeginning: true });
  console.log('Shipping Service Kafka Consumer subscribed to "saga-commands"');

  await consumer.run({
    eachMessage: async ({ message }) => {
      let command;
      try {
        command = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('[Shipping Service Kafka] Failed to parse message:', err);
        return;
      }

      if (!['CREATE_SHIPMENT', 'CANCEL_SHIPMENT'].includes(command.type)) {
        return;
      }

      console.log(`[Shipping Service Kafka] Processing command: ${command.type} for Saga ${command.sagaId}`);

      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');

        // 1. Idempotency Check: insert event ID
        await trackEventId(client, command.event_id);

        switch (command.type) {
          case 'CREATE_SHIPMENT': {
            const { orderId, address } = command;

            // Business logic: insert shipment
            await client.query(
              "INSERT INTO shipments (order_id, address, status) VALUES ($1, $2, 'SHIPPED')",
              [orderId, address]
            );

            // Queue success event
            await createOutboxEvent(client, 'saga-events', 'SHIPMENT_CREATED', {
              sagaId: command.sagaId,
              orderId
            });
            break;
          }

          case 'CANCEL_SHIPMENT': {
            const { orderId } = command;

            // Business logic: cancel shipment
            await client.query(
              "UPDATE shipments SET status = 'CANCELLED' WHERE order_id = $1",
              [orderId]
            );

            // Queue success event
            await createOutboxEvent(client, 'saga-events', 'SHIPMENT_CANCELLED', {
              sagaId: command.sagaId,
              orderId
            });
            break;
          }
        }

        await client.query('COMMIT');
        console.log(`[Shipping Service Kafka] Command ${command.type} for Saga ${command.sagaId} committed.`);
      } catch (err) {
        await client.query('ROLLBACK');
        if (err.code === '23505') {
          console.log(`[Shipping Service Kafka Idempotency] Duplicate event detected. Event "${command.event_id}" already processed.`);
        } else {
          console.error(`[Shipping Service Kafka] Error executing transaction for command ${command.type}:`, err);
        }
      } finally {
        client.release();
      }
    }
  });
};

module.exports = { startShippingConsumer };
