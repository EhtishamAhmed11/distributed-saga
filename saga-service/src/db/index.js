const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const query = (text, params) => pool.query(text, params);

const initDb = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS sagas (
      id SERIAL PRIMARY KEY,
      order_id INTEGER,
      current_step VARCHAR(50) NOT NULL DEFAULT 'STARTED',
      status VARCHAR(50) NOT NULL DEFAULT 'PROCESSING',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  const alterTableQuery1 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS current_step VARCHAR(50) NOT NULL DEFAULT 'STARTED';
  `;
  const alterTableQuery2 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
  `;
  const alterTableQuery3 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS last_error TEXT;
  `;
  const alterTableQuery4 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMP;
  `;
  const alterTableQuery5 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS product_id VARCHAR(255);
  `;
  const alterTableQuery6 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS quantity INTEGER;
  `;
  const alterTableQuery7 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS total_price NUMERIC(10, 2);
  `;
  const alterTableQuery8 = `
    ALTER TABLE sagas ADD COLUMN IF NOT EXISTS address VARCHAR(255);
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
    await query(alterTableQuery1);
    await query(alterTableQuery2);
    await query(alterTableQuery3);
    await query(alterTableQuery4);
    await query(alterTableQuery5);
    await query(alterTableQuery6);
    await query(alterTableQuery7);
    await query(alterTableQuery8);
    await query(createOutboxTable);
    await query(createProcessedEventsTable);
    console.log('Saga database initialized (sagas, outbox, and processed tables ready)');
  } catch (err) {
    console.error('Failed to initialize Saga database:', err);
  }
};

module.exports = {
  query,
  pool,
  initDb
};
