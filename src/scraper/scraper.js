const { initBrowser } = require('../config/puppeteer');
const { readCSVFile } = require('../utils/csvReader');
const { interactWithPage } = require('./pageInteractor');
const { interceptResponses } = require('./trafficInterceptor');

async function runScraper() {
    const urls = await readCSVFile('./cities.csv'); // Carga las URLs desde el CSV
    const browser = await initBrowser();

    for (const { url, city } of urls) {
        const page = await browser.newPage();
        try {
            await page.goto(url, { waitUntil: 'load', timeout: 0 });
            await interactWithPage(page);
            await interceptResponses(page, city);
        } catch (error) {
            console.error(`Error procesando la ciudad ${city} (${url}):`, error);
        } finally {
            await page.close();
        }
    }

    await browser.close();
}

module.exports = { runScraper };
