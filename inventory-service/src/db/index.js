const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const query = (text, params) => pool.query(text, params);

const initDb = async () => {
  const createProductsTable = `
    CREATE TABLE IF NOT EXISTS products (
      id VARCHAR(255) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      stock INTEGER NOT NULL CHECK (stock >= 0),
      price NUMERIC(10, 2) NOT NULL
    );
  `;
  
  const createReservationsTable = `
    CREATE TABLE IF NOT EXISTS reservations (
      id SERIAL PRIMARY KEY,
      order_id INTEGER NOT NULL,
      product_id VARCHAR(255) NOT NULL REFERENCES products(id),
      quantity INTEGER NOT NULL,
      status VARCHAR(50) DEFAULT 'ACTIVE',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  const alterReservationsDefault = `
    ALTER TABLE reservations ALTER COLUMN status SET DEFAULT 'ACTIVE';
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
    await query(createProductsTable);
    await query(createReservationsTable);
    await query(alterReservationsDefault);
    await query(createOutboxTable);
    await query(createProcessedEventsTable);
    console.log('Inventory database tables initialized (status default ACTIVE, outbox & processed ready)');
    
    // Seed data if catalog is empty
    const productCountResult = await query('SELECT COUNT(*) FROM products');
    if (parseInt(productCountResult.rows[0].count, 10) === 0) {
      await query(`
        INSERT INTO products (id, name, stock, price) VALUES
        ('PROD-001', 'Premium Laptop', 10, 999.99),
        ('PROD-002', 'Wireless Mouse', 50, 49.99),
        ('PROD-003', 'Mechanical Keyboard', 20, 129.99)
      `);
      console.log('Seed products inserted into Inventory database');
    }
  } catch (err) {
    console.error('Failed to initialize Inventory database:', err);
    throw err;
  }
};

module.exports = {
  query,
  pool,
  initDb
};
