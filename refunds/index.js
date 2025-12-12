const fs = require('fs');
const csv = require('csv-parser');
const axios = require('axios');

const API_URL = 'https://asia-south1-rupiseva.cloudfunctions.net/refundFailedBillPayments';
const CSV_FILE = '/Users/chethanraos/work_repos/scripts/refunds/refunds.csv';

async function processRefunds() {
  const transactionIds = [];
  
  // Read CSV file and collect transaction IDs
  return new Promise((resolve, reject) => {
    fs.createReadStream(CSV_FILE)
      .pipe(csv())
      .on('data', (row) => {
        // Get the first column value (txnId)
        const txnId = row.txnId || Object.values(row)[0];
        if (txnId && txnId.trim() !== '') {
          transactionIds.push(txnId.trim());
        }
      })
      .on('end', async () => {
        console.log(`Found ${transactionIds.length} transaction IDs to process`);
        
        // Process each transaction ID
        for (let i = 0; i < transactionIds.length; i++) {
          const txnId = transactionIds[i];
          try {
            console.log(`Processing ${i + 1}/${transactionIds.length}: ${txnId}`);
            
            const response = await axios.post(API_URL, {
              transactionIds: [txnId],
              agent: 'Automated'
            }, {
              headers: {
                'Content-Type': 'application/json'
              }
            });
            try {
              console.log(`✓ Success for ${txnId}:`, response.data.data[0].refundResponse.msg);
            } catch (error) {
              console.error(`✗ Error for ${txnId}:`, error.response?.status || error.message);
              console.log(response.data);
            }
          } catch (error) {
            console.error(`✗ Error for ${txnId}:`, error.response?.status || error.message);
            if (error.response?.data) {
              console.error('  Response data:', error.response.data);
            }
          }
          
          // Small delay to avoid overwhelming the API
          await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        console.log('\nProcessing complete!');
        resolve();
      })
      .on('error', (error) => {
        console.error('Error reading CSV file:', error);
        reject(error);
      });
  });
}

// Run the script
processRefunds().catch(console.error);



















