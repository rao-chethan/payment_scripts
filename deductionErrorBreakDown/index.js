const admin = require("firebase-admin");
const fs = require("fs");
const path = require("path");

admin.initializeApp({ projectId: "rupiseva" });

const db = admin.firestore();

const GOOD_SCORE_COLLECTION = "goodscore-deduction-data";
const TRANSACTIONS_COLLECTION = "transactions";
const TRANSACTION_BATCH_SIZE = 200;

const outputFilePath = path.join(__dirname, "notification-response-counts.csv");

const chunkArray = (arr, size) => {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
};

async function fetchGoodscoreDocs() {
  const snapshot = await db
    .collection(GOOD_SCORE_COLLECTION)
    .where("pg", "==", "phonepe")
    .where("status", "==", "FAILED")
    .get();

  if (snapshot.empty) {
    console.log("No deduction docs found for the given filters.");
  } else {
    console.log(`Fetched ${snapshot.size} deduction docs`);
  }

  return snapshot.docs;
}

async function fetchTransactionsMap(transactionIds) {
  const idToData = new Map();
  const chunks = chunkArray(transactionIds, TRANSACTION_BATCH_SIZE);

  for (const chunk of chunks) {
    const lookups = chunk.map(async (transactionId) => {
      const snapshot = await db
        .collection(TRANSACTIONS_COLLECTION)
        .where("parentAutopayId", "==", transactionId)
        .orderBy("createdAt", "desc")
        .limit(1)
        .get();

      if (!snapshot.empty) {
        idToData.set(transactionId, snapshot.docs[0].data());
      }
    });

    await Promise.all(lookups);
  }

  return idToData;
}

function getNotificationKey(notificationResponse = {}) {
  const code = notificationResponse.code ?? "NO_CODE";
  const message = notificationResponse.message ?? "NO_MESSAGE";
  return `${code}|||${message}`;
}

function writeCountsToCsv(counts) {
  const rows = ["code,message,count"];
  counts.forEach(({ code, message, count }) => {
    const safeCode = typeof code === "string" ? code.replace(/"/g, '""') : code;
    const safeMessage = typeof message === "string" ? message.replace(/"/g, '""') : message;
    rows.push(`"${safeCode}","${safeMessage}",${count}`);
  });

  fs.writeFileSync(outputFilePath, rows.join("\n"), "utf8");
  console.log(`Saved summary to ${outputFilePath}`);
}

async function runQuery() {
  try {
    const deductionDocs = await fetchGoodscoreDocs();

    const transactionIds = deductionDocs
      .map((doc) => doc.get("transactionId"))
      .filter((id) => typeof id === "string" && id.trim().length > 0);

    if (transactionIds.length === 0) {
      console.log("No transactionIds found in the selected documents.");
      return;
    }

    const transactionsMap = await fetchTransactionsMap(transactionIds);
    const counts = new Map();

    for (const doc of deductionDocs) {
      const transactionId = doc.get("transactionId");
      if (!transactionId) continue;

      const transactionData = transactionsMap.get(transactionId);
      if (!transactionData) continue;

      const notificationResponse = transactionData.notificationResponse || {};
      const key = getNotificationKey(notificationResponse);
      counts.set(key, (counts.get(key) || 0) + 1);
    }

    if (counts.size === 0) {
      console.log("No notification responses found for the retrieved transactions.");
      return;
    }

    const summary = Array.from(counts.entries()).map(([key, count]) => {
      const [code, message] = key.split("|||");
      return { code, message, count };
    });

    summary.sort((a, b) => b.count - a.count);

    console.table(summary);
    writeCountsToCsv(summary);
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

