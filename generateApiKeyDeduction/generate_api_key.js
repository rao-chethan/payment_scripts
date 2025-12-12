
const bcrypt = require('bcrypt');
const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function generateApiKeyHash() {
  try {
    let secretKey;

    // Check if secret key is provided as command line argument
    if (process.argv[2]) {
      secretKey = process.argv[2];
    } else {
      // Prompt user for secret key
      secretKey = await new Promise((resolve) => {
        rl.question('Enter your API secret key (or press Enter for default): ', (answer) => {
          resolve(answer || 'OVeRw760cnrIGNO7ra6Pa40P-cRsdKH30KtLP9I1Tk9');
        });
      });
    }

    const saltRounds = 10; // Standard security level

    console.log('\n🔐 Generating API Key Hash...');
    console.log(`Secret Key: ${secretKey}`);
    console.log(`Salt Rounds: ${saltRounds}`);

    // Generate the hash
    const hashedKey = await bcrypt.hash(secretKey, saltRounds);
    
    console.log('\n✅ Hash generated successfully!');
    console.log('\n📋 Copy the following to your environment variables:');
    console.log('─'.repeat(80));
    console.log(`API_KEY_HASH="${hashedKey}"`);
    console.log('─'.repeat(80));
    
    console.log('\n📝 Instructions:');
    console.log('1. Add the above line to your .env file or server configuration');
    console.log('2. Make sure to keep your original secret key secure');
    console.log('3. The hash can be safely stored in environment variables');
    console.log('4. Use the original secret key in the x-api-key header when making requests');
    
    console.log('\n🧪 Test your setup:');
    console.log('You can test the hash with the following curl command:');
    console.log(`curl -X POST http://localhost:3000/v1/sendRedeductNotification \\`);
    console.log(`  -H "Content-Type: application/json" \\`);
    console.log(`  -H "x-api-key: ${secretKey}" \\`);
    console.log(`  -d '{"isTest": true}'`);
    
  } catch (error) {
    console.error('❌ Error generating hash:', error.message);
    process.exit(1);
  } finally {
    rl.close();
  }
}

// Run the script
generateApiKeyHash();
