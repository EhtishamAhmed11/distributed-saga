const { randomUUID } = require('crypto');
const { publishEvent } = require('./kafka');

/**
 * Inserts an event into the outbox table using a provided client (within a transaction)
 * @param {import('pg').PoolClient} client 
 * @param {string} topic 
 * @param {string} eventType 
 * @param {Object} payload 
 */
const createOutboxEvent = async (client, topic, eventType, payload) => {
  const eventId = randomUUID();
  await client.query(
    "INSERT INTO outbox (event_id, topic, event_type, payload) VALUES ($1, $2, $3, $4)",
    [eventId, topic, eventType, JSON.stringify(payload)]
  );
};

/**
 * Starts a background scanner that processes pending outbox events using FOR UPDATE SKIP LOCKED
 * @param {Object} db 
 * @param {number} intervalMs 
 */
const startOutboxWorker = (db, intervalMs = 2000) => {
  setInterval(async () => {
    try {
      const result = await db.query(
        "SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY id ASC LIMIT 5 FOR UPDATE SKIP LOCKED"
      );

      if (result.rows.length === 0) {
        return;
      }

      for (const row of result.rows) {
        try {
          // Publish the event to Kafka, ensuring event_id is attached to the payload
          await publishEvent(row.topic, row.event_type, {
            event_id: row.event_id,
            ...row.payload
          });

          // Mark as SENT
          await db.query(
            "UPDATE outbox SET status = 'SENT' WHERE id = $1",
            [row.id]
          );
        } catch (err) {
          console.error(`[Outbox Worker] Failed to process outbox row ${row.id}:`, err.message);
        }
      }
    } catch (err) {
      console.error('[Outbox Worker] Error scanning outbox:', err.message);
    }
  }, intervalMs);
};

/**
 * Registers the incoming event ID to prevent duplicate handling.
 * Throws a unique constraint violation error if the event was already processed.
 * @param {import('pg').PoolClient} client 
 * @param {string} eventId 
 */
const trackEventId = async (client, eventId) => {
  if (!eventId) return; // Allow manual event triggers without IDs
  await client.query(
    "INSERT INTO processed_events (event_id) VALUES ($1)",
    [eventId]
  );
};

module.exports = {
  createOutboxEvent,
  startOutboxWorker,
  trackEventId
};
