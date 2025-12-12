const express = require('express');
const app = express();
const {createPayuUpiConsent} = require('../payuCreateSubscription/index');

// Middleware to parse JSON bodies
app.use(express.json());

// CORS middleware (optional, for cross-origin requests)
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

// PayU UPI Consent Creation endpoint
app.post('/payu/create-upi-consent', async (req, res) => {
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
                             error.message?.includes('must be') ? 400 : 500;
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

// PayU Refund Webhook endpoint
app.post('/goodscore-staging/asia-south1/payuRefundWebhook', (req, res) => {
    console.log('PayU Refund Webhook received:', req.body);
    
    // Process the webhook data here
    // For now, just return a success response
    res.status(200).json({ 
        success: true, 
        message: 'Webhook received successfully' 
    });
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok' });
});

// Start server on port 5001
const PORT = 5001;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log(`PayU UPI Consent endpoint: http://localhost:${PORT}/payu/create-upi-consent`);
    console.log(`PayU Refund Webhook endpoint: http://localhost:${PORT}/goodscore-staging/asia-south1/payuRefundWebhook`);
    console.log(`Health check: http://localhost:${PORT}/health`);
});


