const { runScraper } = require('./scraper/scraper');

(async () => {
    try {
        console.log('Iniciando scraper...');
        await runScraper();
        console.log('Scraper finalizado correctamente.');
    } catch (error) {
        console.error('Error ejecutando el scraper:', error);
    }
})();
