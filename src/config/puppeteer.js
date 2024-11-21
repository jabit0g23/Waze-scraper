const puppeteer = require('puppeteer');

async function initBrowser() {
    const browser = await puppeteer.launch({
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });
    return browser;
}

module.exports = { initBrowser };
