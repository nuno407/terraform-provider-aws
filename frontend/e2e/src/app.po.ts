/*
 * Copyright (C) 2019 Robert Bosch GmbH Copyright (C) 2019 Robert Bosch Manufacturing Solutions GmbH, Germany. All rights reserved.
 */

import { browser, by, element } from 'protractor';

export class AppPage {
  navigateTo() {
    return browser.get('/');
  }

  getTitle() {
    return element(by.css('.sidenav-header h1')).getText();
  }
}
