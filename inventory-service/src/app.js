const express = require('express');
const dotenv = require('dotenv');
const db = require('./db');
const inventoryRoutes = require('./routes/inventoryRoutes');

const { connectKafka } = require('./utils/kafka');
const { startInventoryConsumer } = require('./utils/kafkaConsumer');
const { startOutboxWorker } = require('./utils/outbox');

dotenv.config();

const app = express();

app.use(express.json());

// Initialize Database & Kafka
db.initDb()
  .then(async () => {
    await connectKafka();
    await startInventoryConsumer().catch(err => {
      console.error('Failed to start Inventory Service Kafka consumer:', err);
    });
    startOutboxWorker(db);
  })
  .catch(err => {
    console.error('Database initialization failed:', err);
  });

app.use('/inventory', inventoryRoutes);

// Basic health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'UP', service: 'inventory-service' });
});

module.exports = app;
