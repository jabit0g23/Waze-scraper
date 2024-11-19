const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'waze-app',
    brokers: ['localhost:9092'], // Configura según sea necesario
});

module.exports = kafka;
