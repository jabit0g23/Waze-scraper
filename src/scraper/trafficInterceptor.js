const { processTrafficData } = require('../services/trafficProcessor');

async function interceptResponses(page, city) {
    let lastResponse = null;

    // Escucha las respuestas de la red
    page.on('response', async (response) => {
        const url = response.url();
        if (url.includes('/api/georss')) {
            lastResponse = response;
        }
    });

    // Espera un tiempo razonable para que las respuestas relevantes lleguen
    await new Promise(resolve => setTimeout(resolve, 10000));

    if (lastResponse) {
        try {
            const data = await lastResponse.json().catch(() => null);
            if (data) {
                console.log(`Procesando datos de tráfico para ${city}`);
                await processTrafficData(data, city);
            } else {
                console.error('No se pudo procesar la última respuesta (JSON no válido)');
            }
        } catch (error) {
            console.error('Error al procesar la última respuesta:', error);
        }
    } else {
        console.log('No se interceptaron respuestas relevantes.');
    }
}

module.exports = { interceptResponses };
