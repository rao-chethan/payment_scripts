const admin = require("firebase-admin");
admin.initializeApp({ projectId: "goodscore-staging" });

const db = admin.firestore();

// Helper function to filter and deduplicate documents
function filterAndDeduplicateDocuments(documents) {
  const seenParentAutopayIds = new Set();
  const filteredDocuments = [];
  let duplicatesRemoved = 0;
  let constraintFiltered = 0;

  for (const doc of documents) {
    const transactionData = doc.data();
    const parentAutopayId = transactionData.parentAutopayId;
    const deductionMonth = transactionData.autopayInfo?.deductionMonth;
    const cycle = transactionData.autopayInfo?.cycle;

    // Check for duplicates by parentAutopayId
    if (seenParentAutopayIds.has(parentAutopayId)) {
      duplicatesRemoved++;
      continue;
    }

    // Apply constraints: deductionMonth must be "October_2025" and cycle must be 1
    if (deductionMonth !== "October_2025" || cycle !== 1) {
      constraintFiltered++;
      continue;
    }

    // Document passes all filters
    seenParentAutopayIds.add(parentAutopayId);
    filteredDocuments.push({
      docId: doc.id,
      parentAutopayId,
      deductionMonth,
      cycle
    });
  }

  return {
    filteredDocuments,
    duplicatesRemoved,
    constraintFiltered,
    totalFiltered: duplicatesRemoved + constraintFiltered
  };
}

// Helper function to save a single document
async function saveDocument(docData, docIndex, chunkNumber) {
  try {
    const retryDocRef = db.collection('goodscore-subscription-retries').doc();
    
    await retryDocRef.set({
      deductionMonth: docData.deductionMonth,
      cycle: docData.cycle,
      parentAutopayId: docData.parentAutopayId,
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });

    return { success: true, docIndex, parentAutopayId: docData.parentAutopayId };
  } catch (error) {
    console.error(`Error saving document ${docIndex} in Chunk ${chunkNumber}:`, error);
    return { success: false, docIndex, error, parentAutopayId: docData.parentAutopayId };
  }
}

// Helper function to process a chunk of 1000 documents concurrently
async function processChunk(documents, chunkNumber) {
  console.log(`Chunk ${chunkNumber}: Starting filtering and deduplication...`);
  
  // Filter and deduplicate documents
  const filterResult = filterAndDeduplicateDocuments(documents);

  // If no documents remain after filtering, skip processing
  if (filterResult.filteredDocuments.length === 0) {
    console.log(`Chunk ${chunkNumber}: No documents to process after filtering`);
    return { 
      successfulDocuments: 0, 
      totalDocuments: 0, 
      totalDocumentsProcessed: 0,
      duplicatesRemoved: filterResult.duplicatesRemoved,
      constraintFiltered: filterResult.constraintFiltered
    };
  }

  console.log(`Chunk ${chunkNumber}: Processing ${filterResult.filteredDocuments.length} filtered documents concurrently`);

  // Process all documents in this chunk concurrently - each document saved individually
  const documentPromises = filterResult.filteredDocuments.map((docData, index) => 
    saveDocument(docData, index + 1, chunkNumber)
  );

  const results = await Promise.allSettled(documentPromises);
  
  // Process results
  let successfulDocuments = 0;
  let totalDocumentsProcessed = 0;
  let failedDocuments = [];
  
  results.forEach((result, index) => {
    if (result.status === 'fulfilled' && result.value.success) {
      successfulDocuments++;
      totalDocumentsProcessed++;
    } else {
      const error = result.status === 'rejected' ? result.reason : result.value.error;
      const parentAutopayId = result.status === 'fulfilled' ? result.value.parentAutopayId : 'unknown';
      failedDocuments.push({ index: index + 1, parentAutopayId, error });
    }
  });

  if (failedDocuments.length > 0) {
    console.log(`Chunk ${chunkNumber}: ${failedDocuments.length} documents failed to save:`);
    failedDocuments.forEach(({ index, parentAutopayId, error }) => {
      console.log(`  - Document ${index} (parentAutopayId: ${parentAutopayId}): ${error.message || error}`);
    });
  }

  console.log(`Chunk ${chunkNumber} completed: ${successfulDocuments}/${filterResult.filteredDocuments.length} documents saved successfully`);
  
  return { 
    successfulDocuments, 
    totalDocuments: filterResult.filteredDocuments.length, 
    totalDocumentsProcessed,
    duplicatesRemoved: filterResult.duplicatesRemoved,
    constraintFiltered: filterResult.constraintFiltered
  };
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
                    .where("parentAutopayId", "!=", "")
                    .where("createdAt", ">=", new Date("2025-10-02"))
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
    console.log(`Total documents saved: ${totalDocumentsProcessed}`);
    console.log("✅ All chunks processed successfully!");

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
