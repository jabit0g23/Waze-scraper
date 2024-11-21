const { initBrowser } = require('../config/puppeteer');
const { readCSVFile } = require('../utils/csvReader');
const { interactWithPage } = require('./pageInteractor');
const { interceptResponses } = require('./trafficInterceptor');

async function startScraper() {
    console.log('Leyendo archivo CSV...');
    const urls = await readCSVFile('./cities.csv');
    console.log('Archivo CSV leído:', urls);

    const browser = await initBrowser();
    console.log('Navegador iniciado.');

    for (const { url, city } of urls) {
        console.log(`Procesando ${city} (${url})...`);
        const page = await browser.newPage();

        try {
            console.log(`Accediendo a la página de ${city}`);
            await page.goto(url, { waitUntil: 'load', timeout: 0 });
            console.log(`Página cargada: ${url}`);
            
            await interactWithPage(page);
            console.log(`Interacción completada para ${city}`);
            
            await interceptResponses(page, city);
            console.log(`Datos interceptados para ${city}`);
        } catch (error) {
            console.error(`Error procesando ${city} (${url}):`, error);
        } finally {
            await page.close();
        }
    }

    await browser.close();
    console.log('Scraper completado.');
}

module.exports = { startScraper };
