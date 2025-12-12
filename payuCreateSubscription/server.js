const express = require('express');
const {createPayuUpiConsent} = require('./index');

const app = express();

// Middleware to parse JSON bodies
app.use(express.json());

// CORS middleware for cross-origin requests
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
    if (req.method === 'OPTIONS') {
        res.sendStatus(200);
    } else {
        next();
    }
});

// Request logging middleware
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
    next();
});

/**
 * POST /api/payu/create-upi-consent
 * Creates a PayU UPI consent for recurring payments
 * 
 * Request Body:
 * {
 *   "txnid": "string (required)",
 *   "amount": "string (required)",
 *   "firstname": "string (required)",
 *   "email": "string (required)",
 *   "phone": "string (required)",
 *   "merchantId": "string (required)",
 *   "lastname": "string (optional)",
 *   "productinfo": "string (optional, default: 'Loan autopay setup')",
 *   "upiType": "string (optional, 'INTENT' or 'UPI', default: 'INTENT')",
 *   "vpa": "string (required if upiType is 'UPI')",
 *   "siDetails": {
 *     "billingAmount": "string",
 *     "billingCurrency": "string",
 *     "billingCycle": "string",
 *     "billingInterval": "number",
 *     "paymentStartDate": "string (YYYY-MM-DD)",
 *     "paymentEndDate": "string (YYYY-MM-DD)"
 *   },
 *   "address1": "string (optional)",
 *   "address2": "string (optional)",
 *   "city": "string (optional)",
 *   "state": "string (optional)",
 *   "country": "string (optional, default: 'India')",
 *   "zipcode": "string (optional)",
 *   "udf1": "string (optional)",
 *   "udf2": "string (optional)",
 *   "udf3": "string (optional)",
 *   "udf4": "string (optional)",
 *   "udf5": "string (optional)",
 *   "surl": "string (optional, success URL)",
 *   "furl": "string (optional, failure URL)"
 * }
 */
app.post('/api/payu/create-upi-consent', async (req, res) => {
    try {
        const result = await createPayuUpiConsent(req.body);
        res.status(200).json(result);
    } catch (error) {
        console.error('Error creating PayU UPI consent:', error);
        
        // Handle different error types
        if (error.status === false || (error.status !== undefined && typeof error.status !== 'number')) {
            // Error object from createPayuUpiConsent (has status: false)
            // Determine HTTP status based on error message
            const httpStatus = error.message?.includes('Missing required') || 
                             error.message?.includes('is required') || 
                             error.message?.includes('must be') ||
                             error.message?.includes('Invalid') ? 400 : 500;
            res.status(httpStatus).json(error);
        } else if (error.message && !error.status) {
            // Standard Error object (validation errors)
            res.status(400).json({
                status: false,
                success: false,
                message: error.message,
            });
        } else {
            // Unknown error format
            res.status(500).json({
                status: false,
                success: false,
                message: 'Internal server error',
                error: error.toString(),
            });
        }
    }
});

/**
 * GET /api/health
 * Health check endpoint
 */
app.get('/api/health', (req, res) => {
    res.status(200).json({ 
        status: 'ok',
        service: 'PayU Subscription API',
        timestamp: new Date().toISOString()
    });
});

/**
 * GET /
 * Root endpoint with API information
 */
app.get('/', (req, res) => {
    res.status(200).json({
        service: 'PayU UPI Consent API',
        version: '1.0.0',
        endpoints: {
            'POST /api/payu/create-upi-consent': 'Create PayU UPI consent for recurring payments',
            'GET /api/health': 'Health check endpoint',
            'GET /': 'API information'
        }
    });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({
        status: false,
        success: false,
        message: 'Internal server error',
        error: err.message
    });
});

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        status: false,
        message: 'Endpoint not found',
        path: req.path
    });
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log('='.repeat(60));
    console.log(`PayU Subscription API Server`);
    console.log('='.repeat(60));
    console.log(`Server is running on port ${PORT}`);
    console.log(`API Base URL: http://localhost:${PORT}`);
    console.log(`Health Check: http://localhost:${PORT}/api/health`);
    console.log(`Create Consent: http://localhost:${PORT}/api/payu/create-upi-consent`);
    console.log('='.repeat(60));
});

module.exports = app;

