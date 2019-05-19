module.exports = endpointWithDefaults

const merge = require('./merge')
const parse = require('./parse')

function endpointWithDefaults (defaults, route, options) {
  return parse(merge(defaults, route, options))
}
