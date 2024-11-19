const puppeteer = require('puppeteer');

async function initBrowser() {
    return await puppeteer.launch({ headless: false });
}

module.exports = { initBrowser };
