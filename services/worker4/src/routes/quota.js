'use strict';
const express = require('express');
const { getQuotaUsed, quotaSummary } = require('../lib/quota');

const router = express.Router();

router.get('/', async (_req, res) => {
  try {
    const used = await getQuotaUsed();
    res.json(quotaSummary(used));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
