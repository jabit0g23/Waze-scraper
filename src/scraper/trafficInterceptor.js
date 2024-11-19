const { processTrafficData } = require('../services/trafficProcessor');

async function interceptResponses(page, city) {
    let lastResponse = null;

    // Escucha todas las respuestas de la página
    page.on('response', async (response) => {
        const url = response.url();
        if (url.includes('/api/georss')) {
            lastResponse = response; // Guarda la última respuesta que coincide
        }
    });

    // Espera para acumular respuestas
    await new Promise(resolve => setTimeout(resolve, 10000));

    // Procesa la última respuesta si existe
    if (lastResponse) {
        try {
            const data = await lastResponse.json().catch(() => null); // Asegura un JSON válido
            if (data) {
                processTrafficData(data, city);
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
