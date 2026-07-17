const express = require('express');
const dotenv = require('dotenv');
const db = require('./db');
const paymentRoutes = require('./routes/paymentRoutes');

const { connectKafka } = require('./utils/kafka');
const { startPaymentConsumer } = require('./utils/kafkaConsumer');
const { startOutboxWorker } = require('./utils/outbox');

dotenv.config();

const app = express();

app.use(express.json());

// Initialize Database & Kafka
db.initDb()
  .then(async () => {
    await connectKafka();
    await startPaymentConsumer().catch(err => {
      console.error('Failed to start Payment Service Kafka consumer:', err);
    });
    startOutboxWorker(db);
  })
  .catch(err => {
    console.error('Database initialization failed:', err);
  });

app.use('/payments', paymentRoutes);

// Basic health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'UP', service: 'payment-service' });
});

module.exports = app;
