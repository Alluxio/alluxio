const { writeFileSync } = require('fs')

const camelCase = require('lodash.camelcase')
const makeDir = require('make-dir')

const sortKeys = require('sort-keys')
const GHE_VERSIONS = [
  '2.13',
  '2.14',
  '2.15',
  '2.16'
]

function normalize (endpoint) {
  endpoint.idName = camelCase(endpoint.idName.replace(/^edit/, 'update'))
}

GHE_VERSIONS.forEach(version => {
  const routes = require('@octokit/routes/routes/ghe-' + version)
  writeRoutesFiles(version, getRoutes(routes))
})

function getRoutes (routes) {
  Object.keys(routes).forEach(scope => {
    const endpoints = routes[scope]

    // remove legacy endpoints
    const indexes = routes[scope].reduce((result, endpoint, i) => {
      if (/-legacy$/.test(endpoint.idName)) {
        result.unshift(i)
      }

      return result
    }, [])
    indexes.forEach(i => endpoints.splice(i, 1))

    // normalize idName
    endpoints.forEach(normalize)

    // handle some exceptions. TODO: move this into @octokit/routes
    endpoints.forEach(endpoint => {
      // exception for uploadReleaseAssets which passes parameters as header values
      // see https://github.com/octokit/rest.js/pull/1043
      if (endpoint.idName === 'uploadReleaseAsset') {
        const contentLengthParam = endpoint.params.find(param => param.name === 'Content-Length')
        const contentTypeParam = endpoint.params.find(param => param.name === 'Content-Type')
        const indexes = endpoint.params.reduce((result, param, i) => {
          if (['Content-Length', 'Content-Type'].includes(param.name)) {
            result.unshift(i)
          }

          return result
        }, [])
        indexes.forEach(i => endpoint.params.splice(i, 1))

        if (contentLengthParam) {
          endpoint.params.unshift(
            Object.assign(contentLengthParam, { name: 'headers.content-length' })
          )
        }
        endpoint.params.unshift(
          Object.assign(contentTypeParam, { name: 'headers.content-type' }),
          {
            name: 'headers',
            location: 'headers',
            required: true,
            type: 'object',
            description: 'Request headers containing `content-type` and `content-length`'
          }
        )
      }

      // exception for markdown.renderRaw which requires a content-type header
      // see https://github.com/octokit/rest.js/pull/1043
      if (endpoint.idName === 'renderRaw') {
        endpoint.headers = {
          'content-type': 'text/plain; charset=utf-8'
        }
      }
    })
  })

  return routes
}

function writeRoutesFiles (version, routes) {
  const newRoutes = {}

  const endpoints = Object.keys(routes).reduce((result, scope) => {
    const scopeEndpoints = routes[scope]
    scopeEndpoints.forEach(endpoint => {
      endpoint.scope = scope
    })
    return result.concat(scopeEndpoints)
  }, [])

  endpoints.forEach(endpoint => {
    const scope = endpoint.scope

    if (!newRoutes[scope]) {
      newRoutes[scope] = {}
    }

    const idName = endpoint.idName

    // new route
    newRoutes[scope][idName] = {
      method: endpoint.method,
      headers: endpoint.headers,
      params: endpoint.params.reduce((result, param) => {
        result[param.name] = {
          type: param.type
        }
        if (param.allowNull) {
          result[param.name].allowNull = true
        }
        if (param.required) {
          result[param.name].required = true
        }
        if (param.mapTo) {
          result[param.name].mapTo = param.mapTo === 'input' ? 'data' : param.mapTo

          if (result[param.name].mapTo === 'data' === param.name) {
            delete result[param.name].mapTo
          }
        }
        if (param.enum) {
          result[param.name].enum = param.enum
        }
        if (param.regex) {
          result[param.name].validation = param.regex
        }
        return result
      }, {}),
      url: endpoint.path
    }

    const previewHeaders = endpoint.previews
      .filter(preview => preview.required)
      .map(preview => `application/vnd.github.${preview.name}-preview+json`)
      .join(',')

    if (previewHeaders) {
      newRoutes[scope][idName].headers = {
        accept: previewHeaders
      }
    }
  })

  const newRoutesSorted = sortKeys(newRoutes, { deep: true })
  makeDir.sync(`ghe-${version}`)
  writeFileSync(`ghe-${version}/all.json`, JSON.stringify(newRoutesSorted, null, 2) + '\n')
  writeFileSync(`ghe-${version}/enterprise-admin.json`, JSON.stringify(newRoutesSorted.enterpriseAdmin, null, 2) + '\n')
  writeFileSync(`ghe-${version}/index.js`, `module.exports = (octokit) => octokit.registerEndpoints({ enterpriseAdmin: require('./enterprise-admin.json') })\n`)
  writeFileSync(`ghe-${version}/all.js`, `module.exports = (octokit) => octokit.registerEndpoints(require('./all.json'))\n`)

  writeFileSync(`ghe-${version}/README.md`, `# @octokit/plugin-enterprise-rest/ghe-${version}

## Enterprise Administration

\`\`\`js
${Object.keys(newRoutesSorted.enterpriseAdmin).map(methodName => endpointToMethod('enterpriseAdmin', methodName, newRoutesSorted.enterpriseAdmin[methodName])).join('\n')}
\`\`\`

## Others

\`\`\`js
${Object.keys(newRoutesSorted).filter(scope => scope !== 'enterpriseAdmin').map(scope => Object.keys(newRoutesSorted[scope]).map(methodName => endpointToMethod(scope, methodName, newRoutesSorted[scope][methodName])).join('\n')).join('\n')}
\`\`\`
`)
}

function endpointToMethod (scope, methodName, meta) {
  return `octokit.${scope}.${methodName}(${Object.keys(meta.params).filter(param => !/\./.test(param)).join(', ')})`
}
