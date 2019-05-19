module.exports = withDefaults

const endpointWithDefaults = require('./lib/endpoint-with-defaults')
const merge = require('./lib/merge')
const parse = require('./lib/parse')

function withDefaults (oldDefaults, newDefaults) {
  const DEFAULTS = merge(oldDefaults, newDefaults)
  return Object.assign(endpointWithDefaults.bind(null, DEFAULTS), {
    DEFAULTS,
    defaults: withDefaults.bind(null, DEFAULTS),
    merge: merge.bind(null, DEFAULTS),
    parse
  })
}
