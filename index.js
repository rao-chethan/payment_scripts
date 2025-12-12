const admin = require("firebase-admin");

admin.initializeApp({ projectId: "rupiseva" });

const db = admin.firestore();

async function runQuery() {
  try {
    const startDate = new Date("2025-11-01");
    const endDate = new Date("2025-11-30");
    const transactionSnap = await db
              .collection("transactions")
              .where("createdAt", ">=", startDate)
              .where("createdAt", "<=", endDate)
              .where("status", "==", "PAYMENT_CREATED")
              .limit(10)
              .get();

  console.log(transactionSnap.docs.length);
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

