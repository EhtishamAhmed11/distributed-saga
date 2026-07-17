const { kafka } = require('./kafka');
const db = require('../db');
const { createOutboxEvent, trackEventId } = require('./outbox');

const consumer = kafka.consumer({ groupId: 'saga-service-group' });

const startSagaConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'saga-events', fromBeginning: true });
  console.log('Saga Service Kafka Consumer subscribed to "saga-events"');

  await consumer.run({
    eachMessage: async ({ message }) => {
      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('[Saga Orchestrator] Failed to parse event:', err);
        return;
      }

      console.log(`[Saga Orchestrator] Received event: ${event.type} for Saga ${event.sagaId}`);

      const sagaId = event.sagaId;

      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');

        await trackEventId(client, event.event_id);

        const result = await client.query("SELECT * FROM sagas WHERE id = $1 FOR UPDATE", [sagaId]);
        if (result.rows.length === 0) {
          console.error(`Saga ${sagaId} not found for event ${event.type}`);
          await client.query('ROLLBACK');
          client.release();
          return;
        }

        const saga = result.rows[0];

        switch (event.type) {
          case 'ORDER_CREATED': {
            if (saga.current_step === 'STARTED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET order_id = $1, current_step = 'ORDER_CREATED' WHERE id = $2",
                [event.orderId, sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'RESERVE_INVENTORY', {
                sagaId,
                orderId: event.orderId,
                productId: saga.product_id,
                quantity: saga.quantity
              });
            }
            break;
          }

          case 'ORDER_CREATED_FAILED': {
            await client.query(
              "UPDATE sagas SET status = 'FAILED', last_error = $1 WHERE id = $2",
              [event.error, sagaId]
            );
            break;
          }

          case 'INVENTORY_RESERVED': {
            if (saga.current_step === 'ORDER_CREATED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET current_step = $1 WHERE id = $2",
                ['INVENTORY_RESERVED', sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'CHARGE_PAYMENT', {
                sagaId,
                orderId: saga.order_id,
                amount: saga.total_price
              });
            }
            break;
          }

          case 'INVENTORY_RESERVED_FAILED': {
            if (saga.current_step === 'ORDER_CREATED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET status = 'COMPENSATING', last_error = $1 WHERE id = $2",
                [event.error, sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'CANCEL_ORDER', {
                sagaId,
                orderId: saga.order_id
              });
            }
            break;
          }

          case 'PAYMENT_CHARGED': {
            if (saga.current_step === 'INVENTORY_RESERVED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET current_step = $1 WHERE id = $2",
                ['PAYMENT_CHARGED', sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'CREATE_SHIPMENT', {
                sagaId,
                orderId: saga.order_id,
                address: saga.address
              });
            }
            break;
          }

          case 'PAYMENT_CHARGE_FAILED': {
            if (saga.current_step === 'INVENTORY_RESERVED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET status = 'COMPENSATING', last_error = $1 WHERE id = $2",
                [event.error, sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'RELEASE_INVENTORY', {
                sagaId,
                orderId: saga.order_id
              });
            }
            break;
          }

          case 'SHIPMENT_CREATED': {
            if (saga.current_step === 'PAYMENT_CHARGED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET current_step = $1 WHERE id = $2",
                ['SHIPPING_CREATED', sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'CONFIRM_ORDER', {
                sagaId,
                orderId: saga.order_id
              });
            }
            break;
          }

          case 'SHIPMENT_CREATED_FAILED': {
            if (saga.current_step === 'PAYMENT_CHARGED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET status = 'COMPENSATING', last_error = $1 WHERE id = $2",
                [event.error, sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'REFUND_PAYMENT', {
                sagaId,
                orderId: saga.order_id
              });
            }
            break;
          }

          case 'ORDER_CONFIRMED': {
            if (saga.current_step === 'SHIPPING_CREATED' && saga.status === 'PROCESSING') {
              await client.query(
                "UPDATE sagas SET current_step = $1, status = $2 WHERE id = $3",
                ['COMPLETED', 'SUCCESS', sagaId]
              );
              console.log(`[Saga Orchestrator] Saga ID ${sagaId} completed successfully!`);
            }
            break;
          }

          case 'SHIPMENT_CANCELLED': {
            if (saga.status === 'COMPENSATING') {
              await client.query(
                "UPDATE sagas SET current_step = $1 WHERE id = $2",
                ['PAYMENT_CHARGED', sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'REFUND_PAYMENT', {
                sagaId,
                orderId: saga.order_id
              });
            }
            break;
          }

          case 'PAYMENT_REFUNDED': {
            if (saga.status === 'COMPENSATING') {
              await client.query(
                "UPDATE sagas SET current_step = $1 WHERE id = $2",
                ['INVENTORY_RESERVED', sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'RELEASE_INVENTORY', {
                sagaId,
                orderId: saga.order_id
              });
            }
            break;
          }

          case 'INVENTORY_RELEASED': {
            if (saga.status === 'COMPENSATING') {
              await client.query(
                "UPDATE sagas SET current_step = $1 WHERE id = $2",
                ['ORDER_CREATED', sagaId]
              );
              await createOutboxEvent(client, 'saga-commands', 'CANCEL_ORDER', {
                sagaId,
                orderId: saga.order_id
              });
            }
            break;
          }

          case 'ORDER_CANCELLED': {
            if (saga.status === 'COMPENSATING') {
              await client.query(
                "UPDATE sagas SET current_step = $1, status = $2 WHERE id = $3",
                ['STARTED', 'FAILED', sagaId]
              );
              console.log(`[Saga Orchestrator] Saga ID ${sagaId} fully compensated and marked FAILED.`);
            }
            break;
          }
        }

        await client.query('COMMIT');
        console.log(`[Saga Orchestrator] Event ${event.type} for Saga ${sagaId} committed.`);
      } catch (err) {
        await client.query('ROLLBACK');
        if (err.code === '23505') {
          console.log(`[Saga Orchestrator Idempotency] Duplicate event detected. Event "${event.event_id}" already processed.`);
        } else {
          console.error(`[Saga Orchestrator] Error processing saga event:`, err);
        }
      } finally {
        client.release();
      }
    }
  });
};

module.exports = { startSagaConsumer };
