const client = require('../config/cassandra');

async function saveToCassandra(table, data) {
    const query = `INSERT INTO ${table} JSON ?`;
    try {
        await client.execute(query, [JSON.stringify(data)]);
        console.log(`Datos guardados en Cassandra (${table})`);
    } catch (error) {
        console.error(`Error guardando en Cassandra: ${error}`);
    }
}

module.exports = { saveToCassandra };
