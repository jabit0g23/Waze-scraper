const { kafka } = require('./kafka');

const producer = kafka.producer();

const sendToKafka = async (topic, message) => {
  try {
    await producer.connect();
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });
    console.log(`Mensaje enviado a Kafka en el tópico ${topic}`);
  } catch (error) {
    console.error(`Error enviando mensaje a Kafka: ${error}`);
  }
};

module.exports = { sendToKafka };
