const admin = require("firebase-admin");
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

admin.initializeApp({ projectId: "goodscore-staging" });

const db = admin.firestore();

const BATCH_SIZE = 10000;
const CSV_FILE_PATH = './data.csv';

async function readCSVData(filePath, limit = null) {
  return new Promise((resolve, reject) => {
    const results = [];
    let count = 0;
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        if (limit && count >= limit) {
          return;
        }
        results.push(data);
        count++;
      })
      .on('end', () => {
        console.log(`Read ${results.length} records from CSV`);
        resolve(results);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

async function saveBatchToFirestore(batchData, batchNumber) {
  const collection = db.collection('goodscore-subscription-retries');
  
  console.log(`Processing batch ${batchNumber} with ${batchData.length} records...`);
  
  const writePromises = batchData.map((record) => {
    const docRef = collection.doc();
    
    const data = {
      cycle: 1,
      deductionMonth: "October_2025",
      parentAutopayId: record.parent_id
    };
    
    return docRef.set(data);
  });
  
  await Promise.all(writePromises);
  console.log(`Batch ${batchNumber} completed - inserted ${batchData.length} documents`);
  return batchData.length;
}

async function processBatchesSequentially(allData) {
  const batches = [];
  
  for (let i = 0; i < allData.length; i += BATCH_SIZE) {
    const batch = allData.slice(i, i + BATCH_SIZE);
    batches.push(batch);
  }
  
  console.log(`Created ${batches.length} batches to process`);
  
  const results = [];
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`Processing batch ${i + 1}/${batches.length}...`);
    
    const startTime = Date.now();
    const result = await saveBatchToFirestore(batch, i + 1);
    const endTime = Date.now();
    const duration = endTime - startTime;
    console.log(`Batch ${i + 1} completed in ${duration}ms`);
    results.push(result);
  }
  
  return results;
}

async function runQuery() {
  try {
    const csvData = await readCSVData(CSV_FILE_PATH);
    
    if (csvData.length === 0) {
      console.log('No data found in CSV file');
      return;
    }
    
    console.log(`Total records to process: ${csvData.length}`);
    
    const results = await processBatchesSequentially(csvData);
    
    const totalInserted = results.reduce((sum, count) => sum + count, 0);
    console.log(`Successfully processed ${totalInserted} records in ${results.length} batches`);
    
  } catch (error) {
    console.error("Error in runQuery:", error);
    throw error;
  }
}

runQuery()
  .then(() => {
    console.log("Query completed");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Error running query:", error);
    process.exit(1);
  });
