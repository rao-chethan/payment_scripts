const axios = require("axios");
const {generateHash} = require("./helper");

async function createPayuUpiConsent(params) {
  try {
    const {
      txnid,
      amount,
      firstname,
      email,
      phone,
      lastname = "",
      productinfo = "Loan autopay setup",
      merchantId,
      // UPI specific parameters
      upiType = "INTENT", // "INTENT" or "UPI"
      vpa, // Required only for UPI Collect (bankcode=UPI)
      // SI Details (mandatory for recurring payments)
      siDetails = {
        billingAmount: "10",
        billingCurrency: "INR",
        billingCycle: "DAILY",
        billingInterval: 1,
        paymentStartDate: "",
        paymentEndDate: "",
      },
      // Optional address fields
      address1 = "",
      address2 = "",
      city = "",
      state = "",
      country = "India",
      zipcode = "",
      // UDF fields
      udf1 = "",
      udf2 = "",
      udf3 = "",
      udf4 = "",
      udf5 = "",
      // URLs
      surl,
      furl,
    } = params;

    // Validation
    if (!txnid || !amount || !firstname || !email || !phone) {
      throw new Error("Missing required fields: txnid, amount, firstname, email, phone");
    }

    if (!merchantId) {
      throw new Error("merchantId is required");
    }

    // Validate UPI type
    if (upiType !== "INTENT" && upiType !== "UPI") {
      throw new Error("upiType must be either 'INTENT' or 'UPI'");
    }

    // For UPI Collect, VPA is required
    if (upiType === "UPI" && !vpa) {
      throw new Error("vpa is required when upiType is 'UPI'");
    }

    // Get PayU credentials
    const payuKey = "JZPQUm";
    const payuSalt = "yeocCauptFi2VtfV1MZ4LKBOBiTOrrBe";
    const payuBaseUrl = "https://secure.payu.in";

    if (!payuKey || !payuSalt) {
      throw new Error("Invalid merchantId or PayU credentials not configured");
    }

    // Set default URLs if not provided
    const successUrl = surl || "https://apiplayground-response.herokuapp.com/";
    const failureUrl = furl || "https://apiplayground-response.herokuapp.com/";

    // Prepare SI Details with defaults
    const today = new Date();
    const oneYearLater = new Date(today);
    oneYearLater.setFullYear(today.getFullYear() + 1);

    const finalSiDetails = {
      billingAmount: siDetails.billingAmount || amount,
      billingCurrency: siDetails.billingCurrency || "INR",
      billingCycle: siDetails.billingCycle || "MONTHLY",
      billingInterval: siDetails.billingInterval || 1,
      paymentStartDate: siDetails.paymentStartDate ||
        today.toISOString().split("T")[0],
      paymentEndDate: siDetails.paymentEndDate ||
        oneYearLater.toISOString().split("T")[0],
    };

    // Build hash string according to PayU documentation:
    // HASH = SHA512(key|txnid|amount|productinfo|firstname|email|udf1|udf2|udf3|udf4|udf5||||||si_details|SALT)
    const siDetailsString = JSON.stringify(finalSiDetails);
    const hashString = `${payuKey}|${txnid}|${amount}|${productinfo}|${firstname}|${email}|${udf1}|${udf2}|${udf3}|${udf4}|${udf5}||||||${siDetailsString}|`;
    const hash = generateHash(hashString, merchantId);

    // Prepare request parameters
    const requestParams = {
      key: payuKey,
      api_version: "7",
      txnid: txnid,
      amount: amount,
      productinfo: productinfo,
      firstname: firstname,
      email: email,
      phone: phone,
      lastname: lastname,
      surl: successUrl,
      furl: failureUrl,
      hash: hash,
      pg: "UPI",
      bankcode: upiType, // "INTENT" or "UPI"
      si: "1", // Mandatory for consent transaction
      si_details: siDetailsString,
    };

    // Add optional fields
    if (address1) requestParams.address1 = address1;
    if (address2) requestParams.address2 = address2;
    if (city) requestParams.city = city;
    if (state) requestParams.state = state;
    if (country) requestParams.country = country;
    if (zipcode) requestParams.zipcode = zipcode;
    if (udf1) requestParams.udf1 = udf1;
    if (udf2) requestParams.udf2 = udf2;
    if (udf3) requestParams.udf3 = udf3;
    if (udf4) requestParams.udf4 = udf4;
    if (udf5) requestParams.udf5 = udf5;

    // UPI specific parameters
    if (upiType === "UPI" && vpa) {
      requestParams.vpa = vpa;
    } else if (upiType === "INTENT") {
      requestParams.txn_s2s_flow = "4"; // Required for UPI Intent
    }

    // Make POST request to PayU
    const payuUrl = `${payuBaseUrl}/_payment`;
    const formData = new URLSearchParams(requestParams);

    try {
      const payuResponse = await axios.post(
          payuUrl,
          formData.toString(),
          {
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
              "Accept": "application/json",
            },
          },
      );

      const responseData = payuResponse.data;

      // For UPI Intent, the response contains intentURIData that can be used in mobile
      // For UPI Collect, the response contains standard payment details

      // Check if response is successful
      const isSuccess = responseData?.status === "success" ||
                      responseData?.metaData?.txnStatus === "pending" ||
                      responseData?.result?.intentURIData;

      return {
        status: true,
        success: isSuccess,
        data: responseData,
        // For mobile apps - UPI Intent response
        intentURIData: responseData?.result?.intentURIData || null,
        // For mobile apps - UPI Collect response
        mihpayid: responseData?.mihpayid || null,
        paymentSource: responseData?.payment_source || null,
        // Additional info for mobile
        message: isSuccess ?
          "UPI consent transaction initiated successfully" :
          "UPI consent transaction failed",
      };
    } catch (err) {
      console.error("Error calling PayU UPI Consent API", err);
      throw {
        status: false,
        success: false,
        message: err?.message || "Error creating UPI consent transaction",
        error: err?.response?.data || err?.message,
      };
    }
  } catch (err) {
    console.error("Error in createPayuUpiConsent", err);
    throw {
      status: false,
      success: false,
      message: err?.message || "Internal server error",
    };
  }
}

// If running as a script, parse command line arguments or use example
if (require.main === module) {
  // Example usage - can be modified to accept command line arguments
  const params = {
    txnid: process.argv[2] || "TXN" + Date.now(),
    amount: process.argv[3] || "10",
    firstname: process.argv[4] || "Test",
    email: process.argv[5] || "test@example.com",
    phone: process.argv[6] || "9999999999",
    merchantId: process.argv[7] || "default",
    // Add more parameters as needed
  };

  createPayuUpiConsent(params)
      .then((result) => {
        console.log(JSON.stringify(result, null, 2));
        process.exit(0);
      })
      .catch((error) => {
        console.error(JSON.stringify(error, null, 2));
        process.exit(1);
      });
}

module.exports = {
  createPayuUpiConsent,
};

