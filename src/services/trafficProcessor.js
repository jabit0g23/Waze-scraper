const { sendToElastic } = require('./elasticsearchService');

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
            sendToElastic('jams_details', jamDetails.idJam, jamDetails);
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
            sendToElastic('alerts_details', alertDetails.idAlert, alertDetails);
        });
    } else {
        console.log('No se encontraron alertas.');
    }
}

module.exports = { processTrafficData };
