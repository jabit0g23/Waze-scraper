const { runScraper } = require('./scraper/scraper');
const { connectProducer, disconnectProducer } = require('./services/kafkaProducer');

(async () => {
    try {
        console.log('Conectando al productor Kafka...');
        await connectProducer();

        console.log('Iniciando scraper...');
        await runScraper();

        console.log('Scraper finalizado correctamente.');
    } catch (error) {
        console.error('Error ejecutando el scraper:', error);
    } finally {
        console.log('Desconectando del productor Kafka...');
        await disconnectProducer();
        process.exit(0);
    }
})();
