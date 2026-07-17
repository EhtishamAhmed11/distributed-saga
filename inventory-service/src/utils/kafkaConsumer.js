const { kafka } = require('./kafka');
const db = require('../db');
const { createOutboxEvent, trackEventId } = require('./outbox');

const consumer = kafka.consumer({ groupId: 'inventory-service-group' });

const startInventoryConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'saga-commands', fromBeginning: true });
  console.log('Inventory Service Kafka Consumer subscribed to "saga-commands"');

  await consumer.run({
    eachMessage: async ({ message }) => {
      let command;
      try {
        command = JSON.parse(message.value.toString());
      } catch (err) {
        console.error('[Inventory Service Kafka] Failed to parse message:', err);
        return;
      }

      if (!['RESERVE_INVENTORY', 'RELEASE_INVENTORY'].includes(command.type)) {
        return;
      }

      console.log(`[Inventory Service Kafka] Processing command: ${command.type} for Saga ${command.sagaId}`);

      const client = await db.pool.connect();
      try {
        await client.query('BEGIN');

        // 1. Idempotency Check: insert event ID
        await trackEventId(client, command.event_id);

        switch (command.type) {
          case 'RESERVE_INVENTORY': {
            const { orderId, productId, quantity } = command;

            // Check if reservation already exists
            const existingResult = await client.query(
              'SELECT * FROM reservations WHERE order_id = $1 FOR UPDATE',
              [orderId]
            );

            if (existingResult.rows.length > 0) {
              const existing = existingResult.rows[0];
              if (existing.status === 'ACTIVE') {
                // Already reserved! Idempotently queue success
                await createOutboxEvent(client, 'saga-events', 'INVENTORY_RESERVED', {
                  sagaId: command.sagaId,
                  orderId,
                  productId,
                  quantity
                });
              } else {
                // Already released
                await createOutboxEvent(client, 'saga-events', 'INVENTORY_RESERVED_FAILED', {
                  sagaId: command.sagaId,
                  orderId,
                  error: 'Reservation already released'
                });
              }
              break;
            }

            // Lock product and check stock
            const productResult = await client.query(
              'SELECT stock FROM products WHERE id = $1 FOR UPDATE',
              [productId]
            );

            if (productResult.rows.length === 0) {
              await createOutboxEvent(client, 'saga-events', 'INVENTORY_RESERVED_FAILED', {
                sagaId: command.sagaId,
                orderId,
                error: 'Product not found'
              });
              break;
            }

            const currentStock = productResult.rows[0].stock;
            if (currentStock < quantity) {
              await createOutboxEvent(client, 'saga-events', 'INVENTORY_RESERVED_FAILED', {
                sagaId: command.sagaId,
                orderId,
                error: 'Insufficient stock'
              });
              break;
            }

            // Deduct stock
            await client.query(
              'UPDATE products SET stock = stock - $1 WHERE id = $2',
              [quantity, productId]
            );

            // Create reservation
            await client.query(
              "INSERT INTO reservations (order_id, product_id, quantity, status) VALUES ($1, $2, $3, 'ACTIVE')",
              [orderId, productId, quantity]
            );

            // Queue success event
            await createOutboxEvent(client, 'saga-events', 'INVENTORY_RESERVED', {
              sagaId: command.sagaId,
              orderId,
              productId,
              quantity
            });
            break;
          }

          case 'RELEASE_INVENTORY': {
            const { orderId } = command;

            // Find reservation
            const reservationResult = await client.query(
              "SELECT * FROM reservations WHERE order_id = $1 FOR UPDATE",
              [orderId]
            );

            if (reservationResult.rows.length === 0) {
              // No reservation exists to release: return success idempotently
              await createOutboxEvent(client, 'saga-events', 'INVENTORY_RELEASED', {
                sagaId: command.sagaId,
                orderId
              });
              break;
            }

            const reservation = reservationResult.rows[0];

            if (reservation.status === 'RELEASED') {
              // Already released: return success idempotently
              await createOutboxEvent(client, 'saga-events', 'INVENTORY_RELEASED', {
                sagaId: command.sagaId,
                orderId
              });
              break;
            }

            // Mark as RELEASED
            await client.query(
              "UPDATE reservations SET status = 'RELEASED' WHERE id = $1",
              [reservation.id]
            );

            // Restore product stock
            await client.query(
              'UPDATE products SET stock = stock + $1 WHERE id = $2',
              [reservation.quantity, reservation.product_id]
            );

            // Queue success event
            await createOutboxEvent(client, 'saga-events', 'INVENTORY_RELEASED', {
              sagaId: command.sagaId,
              orderId
            });
            break;
          }
        }

        await client.query('COMMIT');
        console.log(`[Inventory Service Kafka] Command ${command.type} for Saga ${command.sagaId} committed.`);
      } catch (err) {
        await client.query('ROLLBACK');
        if (err.code === '23505') {
          console.log(`[Inventory Service Kafka Idempotency] Duplicate event detected. Event "${command.event_id}" already processed.`);
        } else {
          console.error(`[Inventory Service Kafka] Error executing transaction for command ${command.type}:`, err);
        }
      } finally {
        client.release();
      }
    }
  });
};

module.exports = { startInventoryConsumer };
