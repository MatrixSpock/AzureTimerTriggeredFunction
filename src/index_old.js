const { MongoClient } = require('mongodb');
const { BlobServiceClient } = require('@azure/storage-blob');
const { parse } = require('json2csv');
const { Readable } = require('stream');

module.exports = async function (context, myTimer) {
    const timeStamp = new Date().toISOString();
    context.log('JavaScript timer trigger function started:', timeStamp);

    // MongoDB Atlas connection details
    const mongoConnectionString = process.env.MongoDBAtlasConnectionString;
    const dbName = process.env.DatabaseName;
    const collectionName = process.env.CollectionName;

    // Azure Blob Storage connection details
    const blobConnectionString = process.env.AzureBlobStorageConnectionString;
    const containerName = process.env.BlobContainerName;

    let client;

    try {
        context.log('Connecting to MongoDB (JRI)...');
        client = new MongoClient(mongoConnectionString, {
            serverSelectionTimeoutMS: 5000,
            connectTimeoutMS: 10000
        });
        await client.connect();
        context.log('Connected to MongoDB successfully');

        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        context.log('Fetching data from MongoDB...');
        const documents = await collection.find({}).toArray();
        context.log(`Retrieved ${documents.length} documents from MongoDB`);

        if (documents.length === 0) {
            context.log.warn('No documents found in MongoDB. Exiting function.');
            return;
        }

        context.log('Generating CSV...');
        const fields = Object.keys(documents[0]);
        const csv = parse(documents, { fields });
        context.log(`Generated CSV with size: ${csv.length} bytes`);

        context.log('Connecting to Azure Blob Storage...');
        const blobServiceClient = BlobServiceClient.fromConnectionString(blobConnectionString);
        const containerClient = blobServiceClient.getContainerClient(containerName);

        context.log('Checking if container exists...');
        const containerExists = await containerClient.exists();
        if (!containerExists) {
            context.log.error(`Container "${containerName}" does not exist. Please create it first.`);
            return;
        }

        const blobName = `data-export-${timeStamp}.csv`;
        context.log(`Attempting to upload blob: ${blobName}`);
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);

        const streamBuffer = Buffer.from(csv, 'utf-8');
        const readableStream = new Readable();
        readableStream.push(streamBuffer);
        readableStream.push(null);

        await blockBlobClient.uploadStream(readableStream, streamBuffer.length);
        context.log(`CSV file uploaded successfully to Blob Storage: ${blobName}`);

        context.log('Function execution completed successfully');
    } catch (error) {
        context.log.error(`Error occurred: ${error.message}`);
        context.log.error(`Error stack: ${error.stack}`);
        if (error.name === 'MongoTimeoutError') {
            context.log.error('MongoDB connection timed out. Check your network settings and connection string.');
        } else if (error.name === 'MongoNetworkError') {
            context.log.error('MongoDB network error. Ensure your MongoDB Atlas IP whitelist includes your IP address.');
        } else {
            context.log.error('An unexpected error occurred.');
        }
    } finally {
        if (client) {
            try {
                await client.close();
                context.log('MongoDB connection closed');
            } catch (closeError) {
                context.log.error(`Error occurred while closing MongoDB connection: ${closeError.message}`);
            }
        }
    }
};
