const { sendToElastic } = require('./elasticsearchService');
const { sendToKafka } = require('./kafkaProducer');

function processTrafficData(data, city) {
    if (Array.isArray(data.jams)) {
        data.jams.forEach(jam => {
            const jamDetails = {
                idJam: jam.id,
                commune: jam.city,
                streetName: jam.street,
                streetEnd: jam.endNode,
                speedKmh: jam.speedKMH,
                length: jam.length,
                timestamp: new Date().toISOString(),
            };
            // Enviar a Elasticsearch si lo deseas
            // sendToElastic('jams_details', jamDetails.idJam, jamDetails);
            sendToKafka('jams', jamDetails);
        });
    }

    if (Array.isArray(data.alerts)) {
        data.alerts.forEach(alert => {
            const alertDetails = {
                idAlert: alert.id,
                commune: alert.city,
                typeAlert: alert.type,
                streetName: alert.street,
                timestamp: new Date().toISOString(),
            };
            // Enviar a Elasticsearch si lo deseas
            // sendToElastic('alerts_details', alertDetails.idAlert, alertDetails);
            sendToKafka('alerts', alertDetails);
        });
    } else {
        console.log('No se encontraron alertas.');
    }
}

module.exports = { processTrafficData };