const { sendToKafka } = require('../config/kafkaProducer');
const { saveToCassandra } = require('./cassandraService');
const { sendToElastic } = require('./elasticsearchService');

async function processTrafficData(data, city) {
    if (Array.isArray(data.jams)) {
        for (const jam of data.jams) {
            const jamDetails = {
                idJam: jam.id,
                commune: jam.city || city,
                streetName: jam.street || 'Desconocido',
                speedKmh: jam.speedKMH || 0,
                length: jam.length || 0,
                timestamp: new Date().toISOString(),
            };

            try {
                console.log(`Enviando datos de atasco a Kafka: ${JSON.stringify(jamDetails)}`);
                await sendToKafka('traffic_jams', jamDetails);
            } catch (error) {
                console.error('Error enviando datos de atasco a Kafka:', error);
            }

            try {
                console.log(`Guardando datos de atasco en Cassandra: ${JSON.stringify(jamDetails)}`);
                await saveToCassandra('traffic_jams', jamDetails);
            } catch (error) {
                console.error('Error guardando datos de atasco en Cassandra:', error);
            }
        }
    }

    if (Array.isArray(data.alerts)) {
        for (const alert of data.alerts) {
            const alertDetails = {
                idAlert: alert.id,
                commune: alert.city || city,
                typeAlert: alert.type || 'Desconocido',
                streetName: alert.street || 'Desconocido',
                timestamp: new Date().toISOString(),
            };

            try {
                console.log(`Enviando alerta a Kafka: ${JSON.stringify(alertDetails)}`);
                await sendToKafka('traffic_alerts', alertDetails);
            } catch (error) {
                console.error('Error enviando alerta a Kafka:', error);
            }

            try {
                console.log(`Guardando alerta en Elasticsearch: ${JSON.stringify(alertDetails)}`);
                await sendToElastic('alerts_details', alertDetails.idAlert, alertDetails);
            } catch (error) {
                console.error('Error guardando alerta en Elasticsearch:', error);
            }
        }
    }
}

module.exports = { processTrafficData };
