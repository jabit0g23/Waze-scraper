const { sendToKafka } = require('../config/kafkaProducer');
const { saveToCassandra } = require('./cassandraService');
const { sendToElastic } = require('./elasticsearchService');

async function processTrafficData(data, city) {
  if (Array.isArray(data.jams)) {
    for (const jam of data.jams) {
      const jamDetails = {
        idJam: jam.id,
        commune: jam.city,
        streetName: jam.street,
        speedKmh: jam.speedKMH,
        length: jam.length,
        timestamp: new Date().toISOString(),
      };

      await sendToKafka('traffic_jams', jamDetails);
      await saveToCassandra('traffic_jams', jamDetails);
    }
  }

  if (Array.isArray(data.alerts)) {
    for (const alert of data.alerts) {
      const alertDetails = {
        idAlert: alert.id,
        commune: alert.city,
        typeAlert: alert.type,
        streetName: alert.street,
        timestamp: new Date().toISOString(),
      };

      await sendToKafka('traffic_alerts', alertDetails);
      await sendToElastic('alerts_details', alertDetails.idAlert, alertDetails);
    }
  }
}

module.exports = { processTrafficData };
