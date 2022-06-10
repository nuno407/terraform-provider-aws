/*
 * Copyright (C) 2019 Robert Bosch GmbH Copyright (C) 2019 Robert Bosch Manufacturing Solutions GmbH, Germany. All rights reserved.
 */

import { browser, logging } from 'protractor';
import { AppPage } from './app.po';

describe('workspace-project App', () => {
  let page: AppPage;

  beforeEach(() => {
    page = new AppPage();
  });

  it('should display the app title in the menu', () => {
    page.navigateTo();
    page
      .getTitle()
      .then((title) => expect(title.replace('\n', ' ')).toEqual(`RideCare`));
  });

  afterEach(async () => {
    // Assert that there are no errors emitted from the browser
    const logs = await browser.manage().logs().get(logging.Type.BROWSER);
    expect(logs).not.toContain(
      jasmine.objectContaining({
        level: logging.Level.SEVERE,
      } as logging.Entry)
    );
  });
});
