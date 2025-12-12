const crypto = require("crypto");

function generateHash(hashString, merchantId) {
  const payuSalt = "yeocCauptFi2VtfV1MZ4LKBOBiTOrrBe";
  
  if (!payuSalt) {
    throw new Error(`Salt not found for merchantId: ${merchantId}`);
  }

  // Append salt to hash string and generate SHA512 hash
  const finalHashString = hashString + payuSalt;
  const hash = crypto.createHash("sha512").update(finalHashString).digest("hex");
  
  return hash;
}

module.exports = {
  generateHash,
};



