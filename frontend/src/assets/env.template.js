(function(window) {
    window["env"] = window["env"] || {};

    // Environment variables
    window["env"]["environmentName"] = "docker";
    window["env"]["api"] = "${API_URL}";

    // Azure OAuth configuration
    let msalConfig = {
      auth: {
          clientId: "${AZURE_CLIENT_ID}",
          authority: "${AZURE_AUTHORITY}",
          redirectUri: '/',
      },
      cache: {
          cacheLocation: "localStorage",
          storeAuthStateInCookie: false,
      },
  };

    window["env"]["msalConfig"] = msalConfig;

  })(this);
