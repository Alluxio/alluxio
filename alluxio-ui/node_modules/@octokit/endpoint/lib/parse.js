module.exports = toRequestOptions

const urlTemplate = require('url-template')
const addQueryParameters = require('./util/add-query-parameters')
const extractUrlVariableNames = require('./util/extract-url-variable-names')
const omit = require('./util/omit')

function toRequestOptions (options) {
  // https://fetch.spec.whatwg.org/#methods
  let method = options.method.toUpperCase()

  // replace :varname with {varname} to make it RFC 6570 compatible
  let url = options.url.replace(/:([a-z]\w+)/g, '{+$1}')
  let headers = options.headers
  let body
  let parameters = omit(options, ['method', 'baseUrl', 'url', 'headers', 'request'])

  // extract variable names from URL to calculate remaining variables later
  const urlVariableNames = extractUrlVariableNames(url)

  url = urlTemplate.parse(url).expand(parameters)

  if (!/^http/.test(url)) {
    url = options.baseUrl + url
  }

  const omittedParameters = Object.keys(options).filter((option) => urlVariableNames.includes(option)).concat('baseUrl')
  const remainingParameters = omit(parameters, omittedParameters)

  // for GET/HEAD requests, set URL query parameters from remaining parameters
  // for PATCH/POST/PUT/DELETE requests, set request body from remaining parameters
  if (['GET', 'HEAD'].includes(method)) {
    url = addQueryParameters(url, remainingParameters)
  } else {
    if ('data' in remainingParameters) {
      body = remainingParameters.data
    } else {
      if (Object.keys(remainingParameters).length) {
        body = remainingParameters
      } else {
        headers['content-length'] = 0
      }
    }
  }

  // default content-type for JSON if body is set
  if (!headers['content-type'] && typeof body !== 'undefined') {
    headers['content-type'] = 'application/json; charset=utf-8'
  }

  // GitHub expects "content-length: 0" header for PUT/PATCH requests without body.
  // fetch does not allow to set `content-length` header, but we can set body to an empty string
  if (['PATCH', 'PUT'].includes(method) && typeof body === 'undefined') {
    body = ''
  }

  // Only return body/request keys if present
  return Object.assign(
    { method: method, url, headers },
    typeof body !== 'undefined' ? { body } : null,
    options.request ? { request: options.request } : null
  )
}
