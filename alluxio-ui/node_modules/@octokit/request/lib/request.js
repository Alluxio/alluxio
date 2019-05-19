module.exports = request

const isPlainObject = require('is-plain-object')

const mockable = require('./fetch')
const getBuffer = require('./get-buffer-response')
const HttpError = require('./http-error')

function request (requestOptions) {
  if (isPlainObject(requestOptions.body) || Array.isArray(requestOptions.body)) {
    requestOptions.body = JSON.stringify(requestOptions.body)
  }

  let headers = {}
  let status
  let url

  return mockable.fetch(requestOptions.url, Object.assign({
    method: requestOptions.method,
    body: requestOptions.body,
    headers: requestOptions.headers,
    redirect: requestOptions.redirect
  }, requestOptions.request))

    .then(response => {
      url = response.url
      status = response.status

      for (const keyAndValue of response.headers.entries()) {
        headers[keyAndValue[0]] = keyAndValue[1]
      }

      if (status === 204 || status === 205) {
        return
      }

      // GitHub API returns 200 for HEAD requsets
      if (requestOptions.method === 'HEAD') {
        if (status < 400) {
          return
        }

        throw new HttpError(response.statusText, status, headers, requestOptions)
      }

      if (status === 304) {
        requestOptions.url = response.headers.location
        throw new HttpError('Not modified', status, headers, requestOptions)
      }

      if (status >= 400) {
        return response.text()

          .then(message => {
            const error = new HttpError(message, status, headers, requestOptions)

            try {
              Object.assign(error, JSON.parse(error.message))
            } catch (e) {
              // ignore, see octokit/rest.js#684
            }

            throw error
          })
      }

      const contentType = response.headers.get('content-type')
      if (/application\/json/.test(contentType)) {
        return response.json()
      }

      if (!contentType || /^text\/|charset=utf-8$/.test(contentType)) {
        return response.text()
      }

      return getBuffer(response)
    })

    .then(data => {
      return {
        status,
        url,
        headers,
        data
      }
    })

    .catch(error => {
      if (error instanceof HttpError) {
        throw error
      }

      throw new HttpError(error.message, 500, headers, requestOptions)
    })
}
