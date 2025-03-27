const { MongoClient } = require('mongodb');
const path = require('path');
const fs = require('fs');
const winston = require('winston');
const PDFDocument = require('pdfkit');
require('dotenv').config();

// Configuració del logger
const logDirectory = path.join(__dirname, process.env.LOG_FILE_PATH || '../data/logs');
const logFile = path.join(logDirectory, 'exercici2.log');

// Crear directorio si no existeix
if (!fs.existsSync(logDirectory)) {
    fs.mkdirSync(logDirectory, { recursive: true });
}

// Configurar logger
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `${timestamp} [${level.toUpperCase()}]: ${message}`;
        })
    ),
    transports: [
        new winston.transports.Console(), // Mostrar en consola
        new winston.transports.File({ filename: logFile }) // Guardar en archivo
    ]
});

// Funció per generar PDF amb els títols
function generarPDF(titulo, titulos, filePath) {
    const doc = new PDFDocument();

    // Crear el fitxer PDF i escriure'l al directori 'out'
    doc.pipe(fs.createWriteStream(filePath));

    // Afegir títol
    doc.fontSize(16).text(titulo, { align: 'center' }).moveDown(2);

    // Afegir els títols de les preguntes
    titulos.forEach((titulo, index) => {
        doc.fontSize(12).text(`${index + 1}. ${titulo}`).moveDown(0.5);
    });

    // Finalitzar el PDF
    doc.end();

    logger.info(`PDF generat correctament: ${filePath}`);
}

// Funció 1: Calcular la mitjana de ViewCount i comptar els posts amb ViewCount superior a la mitjana
async function calcularMediaViewCount() {
    const uri = process.env.MONGODB_URI || 'mongodb://root:password@localhost:27017/';
    const client = new MongoClient(uri);

    try {
        await client.connect();
        logger.info('Conectado a MongoDB');

        const database = client.db('posts_db');
        const collection = database.collection('posts');

        // Obtenir tots els posts
        const posts = await collection.find().toArray();

        // Calcular la mitjana de ViewCount
        const totalViewCount = posts.reduce((sum, post) => sum + post.ViewCount, 0);
        const avgViewCount = totalViewCount / posts.length;

        // Asegurarse de que es guarda la mitjana correctament
        logger.info(`Mitjana de ViewCount de tots els posts: ${avgViewCount.toFixed(2)}`);

        // Comptar les preguntes amb ViewCount superior a la mitjana
        const count = posts.filter(post => post.ViewCount > avgViewCount).length;

        // Generar el PDF amb la informació obtinguda
        const titulos = [
            `Mitjana de ViewCount: ${avgViewCount.toFixed(2)}`,
            `Número de preguntes amb ViewCount > mitjana: ${count}`
        ];
        const filePath1 = path.join(__dirname, 'data', 'out', 'informe1.pdf');
        generarPDF('Informe sobre la Mitjana de ViewCount', titulos, filePath1);

        logger.info(`Número de preguntes amb ViewCount superior a la mitjana: ${count}`);
        console.log(`Número de preguntes amb ViewCount superior a la mitjana: ${count}`);

    } catch (error) {
        logger.error(`Error en la consulta a MongoDB: ${JSON.stringify(error, null, 2)}`);
    } finally {
        await client.close();
        logger.info('Conexió a MongoDB tancada');
    }
}

// Funció 2: Obtenir els posts el títol dels quals conté alguna de les paraules de l'array
async function buscarPostsPerTitol() {
    const uri = process.env.MONGODB_URI || 'mongodb://root:password@localhost:27017/';
    const client = new MongoClient(uri);

    try {
        await client.connect();
        logger.info('Conectado a MongoDB');

        const database = client.db('posts_db');
        const collection = database.collection('posts');

        // Paraules a cercar en el títol
        const wordsToSearch = ["pug", "wig", "yak", "nap", "jig", "mug", "zap", "gag", "oaf", "elf"];
        const regex = new RegExp(wordsToSearch.join("|"), "i"); // "i" per a cerca insensible a majúscules/minúscules

        // Cercar els posts que contenen alguna de les paraules
        const postsWithMatchingTitle = await collection.find({ title: { $regex: regex } }).toArray();

        // Generar el PDF amb els títols trobats
        const titulos = postsWithMatchingTitle.map(post => post.title);
        const filePath2 = path.join(__dirname, 'data', 'out', 'informe2.pdf');
        generarPDF('Informe de Títols de Preguntes', titulos, filePath2);

        // Mostrar el número de posts trobats
        logger.info(`Número de posts amb títols que coincideixen amb les paraules: ${postsWithMatchingTitle.length}`);
        console.log(`Número de posts amb títols que coincideixen amb les paraules: ${postsWithMatchingTitle.length}`);

        // Mostrar els títols de les preguntes trobades
        postsWithMatchingTitle.forEach(post => {
            console.log(`Títol: ${post.title}`);
        });

    } catch (error) {
        logger.error(`Error en la consulta a MongoDB: ${JSON.stringify(error, null, 2)}`);
    } finally {
        await client.close();
        logger.info('Conexió a MongoDB tancada');
    }
}

// Crear el directori per als PDFs si no existeix
const outDirectory = path.join(__dirname, 'data', 'out');
if (!fs.existsSync(outDirectory)) {
    fs.mkdirSync(outDirectory, { recursive: true });
}

// Executar les funcions
calcularMediaViewCount();
buscarPostsPerTitol();
