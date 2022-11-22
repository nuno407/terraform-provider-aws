import { Configuration, LogLevel } from '@azure/msal-browser';

export const msalConfig: Configuration = {
  auth: {
    clientId: 'b74f0fea-4e4a-4785-886a-96c1922dfb7b',
    authority: 'https://login.microsoftonline.com/0ae51e19-07c8-4e4b-bb6d-648ee58410f4',
    redirectUri: '/',
  },
  cache: {
    cacheLocation: 'localStorage',
    storeAuthStateInCookie: false,
  },
  system: {
    loggerOptions: {
      loggerCallback: (level, message, containsPii) => {
        switch (level) {
          case LogLevel.Error:
            console.log(message);
            return;
          case LogLevel.Info:
            console.info(message);
            return;
          case LogLevel.Verbose:
            console.warn(message);
            return;
          case LogLevel.Warning:
            console.error(message);
            return;
        }
      },
    },
  },
};
