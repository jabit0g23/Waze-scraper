const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
    contactPoints: ['localhost'],
    localDataCenter: 'datacenter1',
    keyspace: 'traffic_data', // Crea este keyspace en Cassandra
});

module.exports = client;
