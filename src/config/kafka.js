const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'waze-app',
    brokers: ['localhost:29092'],
});

module.exports = kafka;
