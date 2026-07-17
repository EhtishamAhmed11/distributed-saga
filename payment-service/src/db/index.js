const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const query = (text, params) => pool.query(text, params);

const initDb = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS payments (
      id SERIAL PRIMARY KEY,
      order_id INTEGER NOT NULL,
      amount NUMERIC(10, 2) NOT NULL,
      status VARCHAR(50) DEFAULT 'COMPLETED',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  const createIdempotencyTable = `
    CREATE TABLE IF NOT EXISTS idempotency_keys (
      key VARCHAR(255) PRIMARY KEY,
      response_status INTEGER NOT NULL,
      response_body JSONB NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  const createOutboxTable = `
    CREATE TABLE IF NOT EXISTS outbox (
      id SERIAL PRIMARY KEY,
      event_id VARCHAR(255) UNIQUE NOT NULL,
      topic VARCHAR(255) NOT NULL,
      event_type VARCHAR(255) NOT NULL,
      payload JSONB NOT NULL,
      status VARCHAR(50) DEFAULT 'PENDING',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  const createProcessedEventsTable = `
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR(255) PRIMARY KEY,
      processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  try {
    await query(createTableQuery);
    await query(createIdempotencyTable);
    await query(createOutboxTable);
    await query(createProcessedEventsTable);
    console.log('Payment database initialized (payments, idempotency_keys, outbox, and processed tables ready)');
  } catch (err) {
    console.error('Failed to initialize Payment database:', err);
    throw err;
  }
};

module.exports = {
  query,
  pool,
  initDb
};
