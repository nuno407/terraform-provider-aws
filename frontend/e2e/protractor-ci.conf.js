/*
 * Copyright (C) 2019 Robert Bosch Manufacturing Solutions GmbH, Germany. All rights reserved.
 */

// Protractor CI configuration file

const config = require('./protractor.conf').config;
process.env.CHROME_BIN =
  process.env.CHROME_BIN || require('puppeteer').executablePath();

config.capabilities = {
  browserName: 'chrome',
  chromeOptions: {
    binary: process.env.CHROME_BIN,
    args: ['--headless', '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--window-size=1000,800']
  }
};

exports.config = config;
