const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const xml2js = require('xml2js');
const winston = require('winston');
require('dotenv').config();

// Configuración del logger
const logDirectory = path.join(__dirname, process.env.LOG_FILE_PATH || '../data/logs');
const logFile = path.join(logDirectory, 'exercici1.log');

// Crear directorio si no existe
if (!fs.existsSync(logDirectory)) {
    fs.mkdirSync(logDirectory, { recursive: true });
}

// Crear logger
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

// Ruta al fitxer XML
const xmlFilePath = path.join(__dirname, '../../data/Posts.xml');

// Funció per llegir i analitzar el fitxer XML
async function parseXMLFile(filePath) {
    try {
        const xmlData = fs.readFileSync(filePath, 'utf-8');
        const parser = new xml2js.Parser({
            explicitArray: false,
            mergeAttrs: true
        });

        return new Promise((resolve, reject) => {
            parser.parseString(xmlData, (err, result) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    } catch (error) {
        logger.error(`Error llegint o analitzant el fitxer XML: ${error.message}`);
        throw error;
    }
}

// Funció per a decode HTML
function decodeHtmlEntities(text) {
    return text.replace(/&lt;/g, "<").replace(/&gt;/g, ">");
}

// Funció per processar les dades i transformar-les a un format més adequat per MongoDB
function processPostsData(data) {
    const posts = Array.isArray(data.posts.row) ? data.posts.row : [data.posts.row];

    const processedPosts = posts.map(row => ({
        Id: row.Id,
        PostTypeId: row.channel,
        AcceptedAnswerId: row.AcceptedAnswerId,
        CreationDate: new Date(row.CreationDate),
        Score: row.Score ? Number(row.Score) : 0,
        ViewCount: row.ViewCount ? Number(row.ViewCount) : 0,
        Body: row.Body || "",
        OwnerUserId: row.OwnerUserId || null,
        LastActivityDate: row.LastActivityDate ? new Date(row.LastActivityDate) : null,
        Title: row.Title || "",
        Tags: row.Tags ? decodeHtmlEntities(row.Tags) : "",
        AnswerCount: row.AnswerCount ? Number(row.AnswerCount) : 0,
        CommentCount: row.CommentCount ? Number(row.CommentCount) : 0,
        ContentLicense: row.ContentLicense || ""
    }));

    // Ordenar per ViewCount (de major a menor) i seleccionar els primers 10.000
    return processedPosts.sort((a, b) => b.ViewCount - a.ViewCount).slice(0, 10000);
}

// Funció principal per carregar les dades a MongoDB
async function loadDataToMongoDB() {
    const uri = process.env.MONGODB_URI || 'mongodb://root:password@localhost:27017/';
    const client = new MongoClient(uri);

    try {
        await client.connect();
        logger.info('Connectat a MongoDB');

        const database = client.db('posts_db');
        const collection = database.collection('posts');

        // Llegir i analitzar el fitxer XML
        logger.info('Llegint el fitxer XML...');
        const xmlData = await parseXMLFile(xmlFilePath);

        // Processar les dades
        logger.info('Processant les dades...');
        const posts = processPostsData(xmlData);

        // Eliminar dades existents (opcional)
        logger.info('Eliminant dades existents...');
        await collection.deleteMany({});

        // Inserir les noves dades
        logger.info('Inserint dades a MongoDB...');
        const result = await collection.insertMany(posts);

        logger.info(`${result.insertedCount} documents inserits correctament.`);
        logger.info('Dades carregades amb èxit!');

    } catch (error) {
        logger.error(`Error carregant les dades a MongoDB: ${error.message}`);
    } finally {
        await client.close();
        logger.info('Connexió a MongoDB tancada');
    }
}

// Executar la funció principal
loadDataToMongoDB();
