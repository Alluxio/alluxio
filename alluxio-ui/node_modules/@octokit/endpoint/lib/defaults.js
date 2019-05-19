const getUserAgent = require('universal-user-agent')

const version = require('../package.json').version
const userAgent = `octokit-endpoint.js/${version} ${getUserAgent()}`

module.exports = {
  method: 'GET',
  baseUrl: 'https://api.github.com',
  headers: {
    accept: 'application/vnd.github.v3+json',
    'user-agent': userAgent
  }
}
