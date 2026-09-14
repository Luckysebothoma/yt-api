'use strict';
require('dotenv/config');
const { google } = require('googleapis');

function ytAuth() {
  const auth = new google.auth.OAuth2(
    process.env.YT_CLIENT_ID,
    process.env.YT_CLIENT_SECRET,
    process.env.YT_REDIRECT_URI,
  );
  auth.setCredentials({ refresh_token: process.env.YT_REFRESH_TOKEN });
  return auth;
}

module.exports = { ytAuth };
