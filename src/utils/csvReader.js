const fs = require('fs');
const csv = require('csv-parser');

async function readCSVFile(filePath) {
    return new Promise((resolve, reject) => {
        const urls = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                if (row.url && row.city) {
                    urls.push({ url: row.url.trim(), city: row.city.trim() });
                }
            })
            .on('end', () => resolve(urls))
            .on('error', reject);
    });
}

module.exports = { readCSVFile };
