const kafka = require('../config/kafka');

const consumer = kafka.consumer({ groupId: 'test-group' });

async function runConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'jams', fromBeginning: true });
    await consumer.subscribe({ topic: 'alerts', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                topic,
                partition,
                offset: message.offset,
                value: message.value.toString(),
            });
        },
    });
}

runConsumer().catch(console.error);
