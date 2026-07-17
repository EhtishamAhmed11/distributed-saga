const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const query = (text, params) => pool.query(text, params);

const initDb = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS shipments (
      id SERIAL PRIMARY KEY,
      order_id INTEGER NOT NULL,
      address VARCHAR(255) NOT NULL,
      status VARCHAR(50) DEFAULT 'SHIPPED',
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
    await query(createOutboxTable);
    await query(createProcessedEventsTable);
    console.log('Shipping database initialized (shipments, outbox, and processed tables ready)');
  } catch (err) {
    console.error('Failed to initialize Shipping database:', err);
    throw err;
  }
};

module.exports = {
  query,
  pool,
  initDb
};
