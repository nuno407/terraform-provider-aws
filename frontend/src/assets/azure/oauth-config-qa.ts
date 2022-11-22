import { Configuration, LogLevel } from '@azure/msal-browser';

export const msalConfig: Configuration = {
  auth: {
    clientId: '58cc8251-bac3-43e4-b50d-025351f73e90',
    authority: 'https://login.microsoftonline.com/0ae51e19-07c8-4e4b-bb6d-648ee58410f4/',
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
