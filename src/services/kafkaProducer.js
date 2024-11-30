const kafka = require('../config/kafka');

const producer = kafka.producer();

async function connectProducer() {
    try {
        await producer.connect();
        console.log('Productor Kafka conectado.');
    } catch (error) {
        console.error('Error al conectar el productor Kafka:', error);
    }
}

async function sendToKafka(topic, message) {
    try {
        await producer.send({
            topic,
            messages: [{ value: JSON.stringify(message) }],
        });
        console.log(`Mensaje enviado al tópico ${topic}`);
    } catch (error) {
        console.error(`Error enviando mensaje a Kafka: ${error}`);
    }
}

async function disconnectProducer() {
    try {
        await producer.disconnect();
        console.log('Productor Kafka desconectado.');
    } catch (error) {
        console.error('Error al desconectar el productor Kafka:', error);
    }
}

module.exports = {
    connectProducer,
    sendToKafka,
    disconnectProducer,
};
