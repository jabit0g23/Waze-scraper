const { Client } = require('@elastic/elasticsearch');

const esClient = new Client({ node: 'http://elasticsearch:9200' });

module.exports = esClient;
