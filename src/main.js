const { createTopics } = require('./config/kafka');
const { startScraper } = require('./scraper/scraper');

(async () => {
  try {
    console.log('Creando topics...');
    await createTopics();
    console.log('Topics creados.');

    console.log('Iniciando scraper...');
    await startScraper();
    console.log('Scraper iniciado.');
  } catch (error) {
    console.error('Error al iniciar la aplicación:', error);
  }
})();
