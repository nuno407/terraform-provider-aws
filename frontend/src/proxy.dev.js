var proxyConfig = [
  {
    context: '/vehicles',
    target: 'https://api.dev.bosch-ridecare.com/dashboard', // todo
    secure: false,
    changeOrigin: true,
  },
];

function setupForCorporateProxy(proxyConfig) {
  return proxyConfig;
}

module.exports = setupForCorporateProxy(proxyConfig);
