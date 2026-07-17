const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'],
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const producer = kafka.producer();

const connectKafka = async () => {
  try {
    await producer.connect();
    console.log('Kafka Producer connected successfully');
  } catch (err) {
    console.error('Failed to connect Kafka Producer:', err);
  }
};

const publishEvent = async (topic, eventType, payload) => {
  try {
    await producer.send({
      topic,
      messages: [
        {
          key: payload.sagaId ? payload.sagaId.toString() : null,
          value: JSON.stringify({
            type: eventType,
            timestamp: new Date().toISOString(),
            ...payload
          })
        }
      ]
    });
    console.log(`[Kafka Publish] Sent event "${eventType}" to topic "${topic}"`);
  } catch (err) {
    console.error(`[Kafka Publish] Error sending event "${eventType}" to topic "${topic}":`, err);
    throw err;
  }
};

module.exports = {
  kafka,
  producer,
  connectKafka,
  publishEvent
};
