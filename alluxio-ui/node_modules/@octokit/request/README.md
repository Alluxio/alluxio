# request.js

> Send parameterized requests to GitHub’s APIs with sensible defaults in browsers and Node

[![@latest](https://img.shields.io/npm/v/@octokit/request.svg)](https://www.npmjs.com/package/@octokit/request)
[![Build Status](https://travis-ci.org/octokit/request.js.svg?branch=master)](https://travis-ci.org/octokit/request.js)
[![Coverage Status](https://coveralls.io/repos/github/octokit/request.js/badge.svg)](https://coveralls.io/github/octokit/request.js)
[![Greenkeeper](https://badges.greenkeeper.io/octokit/request.js.svg)](https://greenkeeper.io/)

`@octokit/request` is a request library for browsers & node that makes it easier
to interact with [GitHub’s REST API](https://developer.github.com/v3/) and
[GitHub’s GraphQL API](https://developer.github.com/v4/guides/forming-calls/#the-graphql-endpoint).

It uses [`@octokit/endpoint`](https://github.com/octokit/endpoint.js) to parse
the passed options and sends the request using [fetch](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API)
 ([node-fetch](https://github.com/bitinn/node-fetch) in Node).

<!-- update table of contents by running `npx markdown-toc README.md -i` -->

<!-- toc -->

- [Usage](#usage)
  * [Node](#node)
  * [Browser](#browser)
  * [REST API example](#rest-api-example)
  * [GraphQL example](#graphql-example)
  * [Alternative: pass `method` & `url` as part of options](#alternative-pass-method--url-as-part-of-options)
- [octokitRequest()](#octokitrequest)
- [`octokitRequest.defaults()`](#octokitrequestdefaults)
- [`octokitRequest.endpoint`](#octokitrequestendpoint)
- [Special cases](#special-cases)
  * [The `data` parameter – set request body directly](#the-data-parameter-%E2%80%93-set-request-body-directly)
  * [Set parameters for both the URL/query and the request body](#set-parameters-for-both-the-urlquery-and-the-request-body)
- [LICENSE](#license)

<!-- tocstop -->

## Usage

### Node

Install with `npm install @octokit/request`.

```js
const octokitRequest = require('@octokit/request')
```

### Browser

1. Download `octokit-request.min.js` from the latest release: https://github.com/octokit/request.js/releases

2. Load it as script into your web application:

   ```html
   <script src="request-rest.min.js"></script>
   ```

3. The `octokitRequest` is now available

### REST API example

```js
// Following GitHub docs formatting:
// https://developer.github.com/v3/repos/#list-organization-repositories
const result = await octokitRequest('GET /orgs/:org/repos', {
  headers: {
    authorization: 'token 0000000000000000000000000000000000000001'
  },
  org: 'octokit',
  type: 'private'
})

console.log(`${result.data.length} repos found.`)
```

### GraphQL example

```js
const result = await octokitRequest('POST /graphql', {
  headers: {
    authorization: 'token 0000000000000000000000000000000000000001'
  },
  query: `query ($login: String!) {
    organization(login: $login) {
      repositories(privacy: PRIVATE) {
        totalCount
      }
    }
  }`,
  variables: {
    login: 'octokit'
  }
})
```

### Alternative: pass `method` & `url` as part of options

Alternatively, pass in a method and a url

```js
const result = await octokitRequest({
  method: 'GET',
  url: '/orgs/:org/repos',
  headers: {
    authorization: 'token 0000000000000000000000000000000000000001'
  },
  org: 'octokit',
  type: 'private'
})
```

## octokitRequest()

`octokitRequest(route, options)` or `octokitRequest(options)`.

**Options**

<table>
  <thead>
    <tr>
      <th align=left>
        name
      </th>
      <th align=left>
        type
      </th>
      <th align=left>
        description
      </th>
    </tr>
  </thead>
  <tr>
    <th align=left>
      <code>route</code>
    </th>
    <td>
      String
    </td>
    <td>
      If <code>route</code> is set it has to be a string consisting of the request method and URL, e.g. <code>GET /orgs/:org</code>
    </td>
  </tr>
  <tr>
    <th align=left>
      <code>options.baseUrl</code>
    </th>
    <td>
      String
    </td>
    <td>
      <strong>Required.</strong> Any supported <a href="https://developer.github.com/v3/#http-verbs">http verb</a>, case insensitive. <em>Defaults to <code>https://api.github.com</code></em>.
    </td>
  </tr>
    <th align=left>
      <code>options.headers</code>
    </th>
    <td>
      Object
    </td>
    <td>
      Custom headers. Passed headers are merged with defaults:<br>
      <em><code>headers['user-agent']</code> defaults to <code>octokit-rest.js/1.2.3</code> (where <code>1.2.3</code> is the released version)</em>.<br>
      <em><code>headers['accept']</code> defaults to <code>application/vnd.github.v3+json</code>.<br>
    </td>
  </tr>
  <tr>
    <th align=left>
      <code>options.method</code>
    </th>
    <td>
      String
    </td>
    <td>
      <strong>Required.</strong> Any supported <a href="https://developer.github.com/v3/#http-verbs">http verb</a>, case insensitive. <em>Defaults to <code>Get</code></em>.
    </td>
  </tr>
  <tr>
    <th align=left>
      <code>options.url</code>
    </th>
    <td>
      String
    </td>
    <td>
      <strong>Required.</strong> A path or full URL which may contain <code>:variable</code> or <code>{variable}</code> placeholders,
      e.g. <code>/orgs/:org/repos</code>. The <code>url</code> is parsed using <a href="https://github.com/bramstein/url-template">url-template</a>.
    </td>
  </tr>
  <tr>
    <th align=left>
      <code>options.data</code>
    </th>
    <td>
      Any
    </td>
    <td>
      Set request body directly instead of setting it to JSON based on additional parameters. See <a href="#data-parameter">"The `data` parameter"</a> below.
    </td>
  </tr>
  <tr>
    <th align=left>
      <code>options.request</code>
    </th>
    <td>
      Object
    </td>
    <td>
     Pass <a href="https://github.com/bitinn/node-fetch#options">node-fetch extensions options</a>, such as <code>agent</code> or <code>timeout</code>. All other `options.request.*` keys will be ignored.
    </td>
  </tr>
</table>

All other options will passed depending on the `method` and `url` options.

1. If the option key is a placeholder in the `url`, it will be used as replacement. For example, if the passed options are `{url: '/orgs/:org/repos', org: 'foo'}` the returned `options.url` is `https://api.github.com/orgs/foo/repos`
2. If the `method` is `GET` or `HEAD`, the option is passed as query parameter
3. Otherwise the parameter is passed in the request body as JSON key.

**Result**

`octokitRequest` returns a promise and resolves with 4 keys

<table>
  <thead>
    <tr>
      <th align=left>
        key
      </th>
      <th align=left>
        type
      </th>
      <th align=left>
        description
      </th>
    </tr>
  </thead>
  <tr>
    <th align=left><code>status</code></th>
    <td>Integer</td>
    <td>Response status status</td>
  </tr>
  <tr>
    <th align=left><code>url</code></th>
    <td>String</td>
    <td>URL of response. If a request results in redirects, this is the final URL. You can send a <code>HEAD</code> request to retrieve it without loading the full response body.</td>
  </tr>
  <tr>
    <th align=left><code>headers</code></th>
    <td>Object</td>
    <td>All response headers</td>
  </tr>
  <tr>
    <th align=left><code>data</code></th>
    <td>Any</td>
    <td>The response body as returned from server. If the response is JSON then it will be parsed into an object</td>
  </tr>
</table>

If an error occurs, the `error` instance has additional properties to help with debugging

- `error.status` The http response status code
- `error.headers` The http response headers as an object
- `error.request` The request options such as `method`, `url` and `data`

## `octokitRequest.defaults()`

Override or set default options. Example:

```js
const myOctokitRequest = require('@octokit/request').defaults({
  baseUrl: 'https://github-enterprise.acme-inc.com/api/v3',
  headers: {
    'user-agent': 'myApp/1.2.3',
    authorization: `token 0000000000000000000000000000000000000001`
  },
  org: 'my-project',
  per_page: 100
})

myOctokitRequest(`GET /orgs/:org/repos`)
```

You can call `.defaults()` again on the returned method, the defaults will cascade.

```js
const myProjectRequest = octokitRequest.defaults({
  baseUrl: 'https://github-enterprise.acme-inc.com/api/v3',
  headers: {
    'user-agent': 'myApp/1.2.3'
  },
  org: 'my-project'
})
const myProjectRequestWithAuth = myProjectRequest.defaults({
  headers: {
    authorization: `token 0000000000000000000000000000000000000001`
  }
})
```

`myProjectRequest` now defaults the `baseUrl`, `headers['user-agent']`,
`org` and `headers['authorization']` on top of `headers['accept']` that is set
by the global default.

## `octokitRequest.endpoint`

See https://github.com/octokit/endpoint.js. Example

```js
const options = octokitRequest.endpoint('GET /orgs/:org/repos', {
  org: 'my-project',
  type: 'private'
})

// {
//   method: 'GET',
//   url: 'https://api.github.com/orgs/my-project/repos?type=private',
//   headers: {
//     accept: 'application/vnd.github.v3+json',
//     authorization: 'token 0000000000000000000000000000000000000001',
//     'user-agent': 'octokit/endpoint.js v1.2.3'
//   }
// }
```

All of the [`@octokit/endpoint`](https://github.com/octokit/endpoint.js) API can be used:

- [`ocotkitRequest.endpoint()`](#endpoint)
- [`ocotkitRequest.endpoint.defaults()`](#endpointdefaults)
- [`ocotkitRequest.endpoint.merge()`](#endpointdefaults)
- [`ocotkitRequest.endpoint.parse()`](#endpointmerge)

## Special cases

<a name="data-parameter"></a>
### The `data` parameter – set request body directly

Some endpoints such as [Render a Markdown document in raw mode](https://developer.github.com/v3/markdown/#render-a-markdown-document-in-raw-mode) don’t have parameters that are sent as request body keys, instead the request body needs to be set directly. In these cases, set the `data` parameter.

```js
const options = endpoint('POST /markdown/raw', {
  data: 'Hello world github/linguist#1 **cool**, and #1!',
  headers: {
    accept: 'text/html;charset=utf-8',
    'content-type': 'text/plain'
  }
})

// options is
// {
//   method: 'post',
//   url: 'https://api.github.com/markdown/raw',
//   headers: {
//     accept: 'text/html;charset=utf-8',
//     'content-type': 'text/plain',
//     'user-agent': userAgent
//   },
//   body: 'Hello world github/linguist#1 **cool**, and #1!'
// }
```

### Set parameters for both the URL/query and the request body

There are API endpoints that accept both query parameters as well as a body. In that case you need to add the query parameters as templates to `options.url`, as defined in the [RFC 6570 URI Template specification](https://tools.ietf.org/html/rfc6570).

Example

```js
octokitRequest('POST https://uploads.github.com/repos/octocat/Hello-World/releases/1/assets{?name,label}', {
  name: 'example.zip',
  label: 'short description',
  headers: {
    'content-type': 'text/plain',
    'content-length': 14,
    authorization: `token 0000000000000000000000000000000000000001`
  },
  data: 'Hello, world!'
})
```



## LICENSE

[MIT](LICENSE)
