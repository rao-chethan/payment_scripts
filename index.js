const admin = require("firebase-admin");
admin.initializeApp({ projectId: "goodscore-staging" });

const db = admin.firestore();

// Helper function to process a single batch within a chunk
async function processBatch(batchData, batchNumber, chunkNumber) {
  try {
    const batch = db.batch();
    
    for (const doc of batchData) {
      const documentId = doc.id;
      const retryDocRef = db.collection('goodscore-subscription-retries').doc();
      
      batch.set(retryDocRef, {
        deductionMonth: "October_2025",
        cycle: 1,
        parentAutopayId: documentId,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    }

    await batch.commit();
    console.log(`Chunk ${chunkNumber} - Batch ${batchNumber} committed successfully (${batchData.length} documents)`);
    return { success: true, batchNumber, count: batchData.length };
  } catch (error) {
    console.error(`Error in Chunk ${chunkNumber} - Batch ${batchNumber}:`, error);
    return { success: false, batchNumber, error };
  }
}

// Helper function to process a chunk of 1000 documents concurrently
async function processChunk(documents, chunkNumber) {
  const batchSize = 500;
  const batches = [];
  
  // Split chunk into batches of 500
  for (let i = 0; i < documents.length; i += batchSize) {
    const batchData = documents.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    batches.push({ data: batchData, batchNumber });
  }

  console.log(`Chunk ${chunkNumber}: Processing ${documents.length} documents in ${batches.length} batches concurrently`);

  // Process all batches in this chunk concurrently
  const batchPromises = batches.map(({ data, batchNumber }) => 
    processBatch(data, batchNumber, chunkNumber)
  );

  const results = await Promise.allSettled(batchPromises);
  
  // Process results
  let successfulBatches = 0;
  let totalDocumentsProcessed = 0;
  
  results.forEach((result, index) => {
    if (result.status === 'fulfilled' && result.value.success) {
      successfulBatches++;
      totalDocumentsProcessed += result.value.count;
    } else {
      console.error(`Chunk ${chunkNumber} - Batch ${batches[index].batchNumber} failed:`, 
        result.status === 'rejected' ? result.reason : result.value.error);
    }
  });

  console.log(`Chunk ${chunkNumber} completed: ${successfulBatches}/${batches.length} batches successful, ${totalDocumentsProcessed} documents processed`);
  
  return { successfulBatches, totalBatches: batches.length, totalDocumentsProcessed };
}

async function runQuery() {
  try {
    console.log("Starting chunked processing for transactions...");
    
    let lastDoc = null;
    let chunkNumber = 0;
    let totalDocumentsProcessed = 0;
    let totalChunksProcessed = 0;
    const chunkSize = 1000;

    while (true) {
      chunkNumber++;
      console.log(`\n=== Processing Chunk ${chunkNumber} ===`);
      
      // Fetch next 1000 documents
      let query = db.collection('transactions')
                    .where("parentAutopayId", "==", "")
                    .where("status", "==", "PAYMENT_SUCCESS")
                    .where("createdAt", ">=", new Date("2025-10-03"))
                    .orderBy("createdAt")
                    .limit(chunkSize);

      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      
      if (snapshot.empty) {
        console.log(`No more documents found. Processing complete.`);
        break;
      }

      const documents = snapshot.docs;
      console.log(`Chunk ${chunkNumber}: Fetched ${documents.length} documents`);

      const chunkResult = await processChunk(documents, chunkNumber);
      
      totalDocumentsProcessed += chunkResult.totalDocumentsProcessed;
      totalChunksProcessed++;
      
      // Update lastDoc for next iteration
      lastDoc = documents[documents.length - 1];
      
      // If we got fewer documents than chunk size, we've reached the end
      if (documents.length < chunkSize) {
        console.log(`Reached end of data. Final chunk processed.`);
        break;
      }
    }

    console.log(`\n=== Final Results ===`);
    console.log(`Total chunks processed: ${totalChunksProcessed}`);
    console.log(`Total documents processed: ${totalDocumentsProcessed}`);
    console.log("All chunks processed successfully!");

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
