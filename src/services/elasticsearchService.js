const esClient = require('../config/elasticsearch');

async function sendToElastic(index, id, body) {
    try {
        await esClient.index({
            index,
            id,
            body,
        });
        console.log(`Datos enviados al índice ${index} con ID ${id}`);
    } catch (error) {
        console.error(`Error enviando datos a ElasticSearch: ${error}`);
    }
}

module.exports = { sendToElastic };
