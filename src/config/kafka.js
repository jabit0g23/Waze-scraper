const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka:29092'],
});

const admin = kafka.admin();

const createTopics = async () => {
  try {
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: 'traffic_jams', numPartitions: 1, replicationFactor: 1 },
        { topic: 'traffic_alerts', numPartitions: 1, replicationFactor: 1 },
      ],
    });
    console.log('Topics creados correctamente.');
  } catch (error) {
    console.error('Error al crear topics:', error);
  } finally {
    await admin.disconnect();
  }
};

module.exports = { kafka, createTopics };
