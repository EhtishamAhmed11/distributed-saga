const express = require('express');
const dotenv = require('dotenv');
const db = require('./db');
const sagaRoutes = require('./routes/sagaRoutes');
const { connectKafka } = require('./utils/kafka');
const { startSagaConsumer } = require('./utils/sagaEventConsumer');
const { startOutboxWorker } = require('./utils/outbox');

dotenv.config();

const app = express();

app.use(express.json());

// Initialize Database & Kafka
db.initDb()
  .then(async () => {
    await connectKafka();
    await startSagaConsumer().catch(err => {
      console.error('Failed to start Saga Service Kafka consumer:', err);
    });
    startOutboxWorker(db);
  })
  .catch(err => {
    console.error('Database initialization failed:', err);
  });

app.use('/saga', sagaRoutes);

// Basic health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'UP', service: 'saga-service' });
});

module.exports = app;
