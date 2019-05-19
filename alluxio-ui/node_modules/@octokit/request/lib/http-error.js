module.exports = class HttpError extends Error {
  constructor (message, statusCode, headers, request) {
    super(message)

    // Maintains proper stack trace (only available on V8)
    /* istanbul ignore next */
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor)
    }

    this.name = 'HttpError'
    this.status = statusCode
    Object.defineProperty(this, 'code', {
      get () {
        console.warn('`error.code` is deprecated, use `error.status`.')
        return statusCode
      }
    })
    this.headers = headers
    this.request = request
  }
}
