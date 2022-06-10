/**
 * Copyright (C) 2019 Robert Bosch Manufacturing Solutions GmbH, Germany. All rights reserved.
 */

// Protractor configuration file, see link for more information
// https://github.com/angular/protractor/blob/master/lib/config.ts

const {SpecReporter} = require('jasmine-spec-reporter');
const jasmineReporters = require('jasmine-reporters');
process.env.CHROME_BIN =
  process.env.CHROME_BIN || require('puppeteer').executablePath();

exports.config = {
  allScriptsTimeout: 11000,
  specs: [
    './src/**/*.e2e-spec.ts'
  ],
  capabilities: {
    browserName: 'chrome',
    chromeOptions: {
      binary: process.env.CHROME_BIN
    }
  },
  directConnect: true,
  baseUrl: 'http://localhost:4200/',
  framework: 'jasmine',
  jasmineNodeOpts: {
    showColors: true,
    defaultTimeoutInterval: 30000,
    print: function () {
    }
  },
  onPrepare() {
    require('ts-node').register({
      project: require('path').join(__dirname, './tsconfig.json')
    });
    jasmine.getEnv().addReporter(new SpecReporter({spec: {displayStacktrace: true}}));
    jasmine.getEnv().addReporter(
      new jasmineReporters.JUnitXmlReporter({
        consolidateAll: true,
        savePath: require('path').join(__dirname, 'results'),
        filePrefix: "e2e-results-junit"
      })
    );
  },
  plugins: [
    {
      package: "protractor-console-plugin",
      failOnWarning: false,
      failOnError: true,
      logWarnings: true
    }
  ]
};
