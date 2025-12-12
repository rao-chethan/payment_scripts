const admin = require("firebase-admin");
const fs = require('fs');
admin.initializeApp({ projectId: "goodscore-staging" });

const db = admin.firestore();

async function runQuery() {
  try {
    // read entry from collection and store in a data.txt file 
    // const collection = db.collection('transactions').doc('8trvB2ToNcIDqdaHWcDQ');
    // const snapshot = await collection.get();
    // const data = snapshot.data();
    // fs.writeFileSync('data.txt', JSON.stringify(data, null, 2));

    // read the data.txt file and get the data and save it to a collection in firestore
    const data = fs.readFileSync('data.txt', 'utf8');
    const item = JSON.parse(data);
    await db.collection('transactions').doc("8trvB2ToNcIDqdaHWcDQ").set(item);
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

