const express = require('express');
const dotenv = require('dotenv');
const db = require('./db');
const orderRoutes = require('./routes/orderRoutes');
const { connectKafka } = require('./utils/kafka');
const { startOrderConsumer } = require('./utils/kafkaConsumer');
const { startOutboxWorker } = require('./utils/outbox');

dotenv.config();

const app = express();

app.use(express.json());

// Initialize Database & Kafka
db.initDb()
  .then(async () => {
    await connectKafka();
    await startOrderConsumer().catch(err => {
      console.error('Failed to start Order Service Kafka consumer:', err);
    });
    startOutboxWorker(db); // Start outbox worker scanning
  })
  .catch(err => {
    console.error('Database initialization failed:', err);
  });

app.use('/orders', orderRoutes);

// Basic health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'UP', service: 'order-service' });
});

module.exports = app;
