const express = require('express');
const dotenv = require('dotenv');
const db = require('./db');
const shippingRoutes = require('./routes/shippingRoutes');

const { connectKafka } = require('./utils/kafka');
const { startShippingConsumer } = require('./utils/kafkaConsumer');
const { startOutboxWorker } = require('./utils/outbox');

dotenv.config();

const app = express();

app.use(express.json());

// Initialize Database & Kafka
db.initDb()
  .then(async () => {
    await connectKafka();
    await startShippingConsumer().catch(err => {
      console.error('Failed to start Shipping Service Kafka consumer:', err);
    });
    startOutboxWorker(db);
  })
  .catch(err => {
    console.error('Database initialization failed:', err);
  });

app.use('/shipping', shippingRoutes);

// Basic health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'UP', service: 'shipping-service' });
});

module.exports = app;
