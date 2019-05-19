const simple = require('simple-mock')
const { test } = require('tap')

test('require("@octokit/enterprise-rest/v2.15")', t => {
  const octokitMock = {
    registerEndpoints: simple.mock()
  }

  const plugin = require('./ghe-2.15')

  plugin(octokitMock)

  t.equals(octokitMock.registerEndpoints.callCount, 1)
  t.end()
})

test('require("@octokit/enterprise-rest/v2.15/all")', t => {
  const octokitMock = {
    registerEndpoints: simple.mock()
  }

  const plugin = require('./ghe-2.15/all')

  plugin(octokitMock)

  t.equals(octokitMock.registerEndpoints.callCount, 1)
  t.end()
})
