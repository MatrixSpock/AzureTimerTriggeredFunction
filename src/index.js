const { MongoClient } = require('mongodb');
const { BlobServiceClient } = require('@azure/storage-blob');
const { parse } = require('json2csv');
const { Readable } = require('stream');

class CustomError extends Error {
    constructor(message, type) {
        super(message);
        this.name = this.constructor.name;
        this.type = type;
    }
}

const connectToMongoDB = async (connectionString, options) => {
    const client = new MongoClient(connectionString, options);
    await client.connect();
    return client;
};

const fetchDataFromMongoDB = async (collection) => {
    const documents = await collection.find({}).toArray();
    if (documents.length === 0) {
        throw new CustomError('No documents found in MongoDB', 'NoDataError');
    }
    return documents;
};

const generateCSV = (documents) => {
    const fields = Object.keys(documents[0]);
    return parse(documents, { fields });
};

const uploadToBlob = async (containerClient, blobName, csv) => {
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    const streamBuffer = Buffer.from(csv, 'utf-8');
    const readableStream = Readable.from(streamBuffer);
    await blockBlobClient.uploadStream(readableStream, streamBuffer.length);
};

const retryOperation = async (operation, maxRetries = 3, delay = 1000) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            if (attempt === maxRetries) throw error;
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
};

module.exports = async function (context, myTimer) {
    const timeStamp = new Date().toISOString();
    context.log('JavaScript timer trigger function started:', timeStamp);

    context.log("Fetching ENV variables");
    const {
        MongoDBAtlasConnectionString,
        DatabaseName,
        CollectionName,
        AzureBlobStorageConnectionString,
        BlobContainerName
    } = process.env;

    // Input validation
    if (!MongoDBAtlasConnectionString || !DatabaseName || !CollectionName || 
        !AzureBlobStorageConnectionString || !BlobContainerName) {
        context.log('Missing required environment variables');
        return;
    }

    let client;

    try {
        context.log("Attempting to connect to MongoDB");
        client = await retryOperation(() => connectToMongoDB(MongoDBAtlasConnectionString, {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 10000
        }));
        context.log('Connected to MongoDB successfully');

        context.log("Fetching DB Name");
        const db = client.db(DatabaseName);
        context.log("Fetching Collection Name");
        const collection = db.collection(CollectionName);
        
        context.log("Fetching documents");
        const documents = await fetchDataFromMongoDB(collection);
        context.log(`Retrieved ${documents.length} documents from MongoDB`);

        const csv = generateCSV(documents);
        context.log(`Generated CSV with size: ${csv.length} bytes`);

        const blobServiceClient = BlobServiceClient.fromConnectionString(AzureBlobStorageConnectionString);
        const containerClient = blobServiceClient.getContainerClient(BlobContainerName);

        const containerExists = await containerClient.exists();
        if (!containerExists) {
            throw new CustomError(`Container "${BlobContainerName}" does not exist`, 'ContainerNotFoundError');
        }

        const blobName = `data-export-${timeStamp}.csv`;
        await uploadToBlob(containerClient, blobName, csv);
        context.log(`CSV file uploaded successfully to Blob Storage: ${blobName}`);

        context.log('Function execution completed successfully');
    } catch (error) {
        context.log(`Error occurred: ${error.message}`);
        context.log(`Error stack: ${error.stack}`);

        if (error instanceof CustomError) {
            context.log(`Custom error type: ${error.type}`);
        } else if (error.name === 'MongoServerSelectionError') {
            context.log('Failed to select a MongoDB server. Check your network settings and connection string.');
        } else if (error.name === 'MongoNetworkError') {
            context.log('MongoDB network error. Ensure your MongoDB Atlas IP whitelist includes your function app\'s IP address.');
        } else {
            context.log('An unexpected error occurred.');
        }
    } finally {
        if (client) {
            try {
                await client.close();
                context.log('MongoDB connection closed');
            } catch (closeError) {
                context.log(`Error occurred while closing MongoDB connection: ${closeError.message}`);
            }
        }
    }
};