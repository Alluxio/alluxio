const endpoint = require('@octokit/endpoint')
const getUserAgent = require('universal-user-agent')

const version = require('./package.json').version
const userAgent = `octokit-request.js/${version} ${getUserAgent()}`
const withDefaults = require('./lib/with-defaults')

module.exports = withDefaults(endpoint, {
  headers: {
    'user-agent': userAgent
  }
})
