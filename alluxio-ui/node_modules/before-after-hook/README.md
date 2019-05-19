# before-after-hook

> asynchronous hooks for internal functionality

[![npm downloads](https://img.shields.io/npm/dw/before-after-hook.svg)](https://www.npmjs.com/package/before-after-hook)
[![Build Status](https://travis-ci.org/gr2m/before-after-hook.svg?branch=master)](https://travis-ci.org/gr2m/before-after-hook)
[![Coverage Status](https://coveralls.io/repos/gr2m/before-after-hook/badge.svg?branch=master)](https://coveralls.io/r/gr2m/before-after-hook?branch=master)
[![Greenkeeper badge](https://badges.greenkeeper.io/gr2m/before-after-hook.svg)](https://greenkeeper.io/)

## Usage

```js
// instantiate hook API
const hook = new Hook()

// Create a hook
function getData (options) {
  return hook('get', options, fetchFromDatabase)
    .then(handleData)
    .catch(handleGetError)
}

// register before/error/after hooks.
// The methods can be async or return a promise
hook.before('get', beforeHook)
hook.error('get', errorHook)
hook.after('get', afterHook)

getData({id: 123})
```

The methods are executed in the following order

1. `beforeHook`
2. `fetchFromDatabase`
3. `afterHook`
4. `getData`

`beforeHook` can mutate `options` before it’s passed to `fetchFromDatabase`.

If an error is thrown in `beforeHook` or `fetchFromDatabase` then `errorHook` is
called next.

If `afterHook` throws an error then `handleGetError` is called instead
of `getData`.

If `errorHook` throws an error then `handleGetError` is called next, otherwise
`afterHook` and `getData`.

You can also use `hook.wrap` to achieve the same thing as shown above:

```js
hook.wrap('get', async (getData, options) => {
  await beforeHook(options)

  try {
    const result = getData(options)
  } catch (error) {
    await errorHook(error, options)
  }

  await afterHook(result, options)
})
```

## Install

```
npm install before-after-hook
```

Or download [the latest `before-after-hook.min.js`](https://github.com/gr2m/before-after-hook/releases/latest).

## API

- [Constructor](#constructor)
- [hook.api](#hookapi)
- [hook()](#hook)
- [hook.before()](#hookbefore)
- [hook.error()](#hookerror)
- [hook.after()](#hookafter)
- [hook.wrap()](#hookwrap)
- [hook.remove()](#hookremove)

### Constructor

The `Hook` constructor has no options and returns a `hook` instance with the
methods below

```js
const hook = new Hook()
```

### hook.api

Use the `api` property to return the public API:

- [hook.before()](#hookbefore)
- [hook.after()](#hookafter)
- [hook.error()](#hookerror)
- [hook.wrap()](#hookwrap)
- [hook.remove()](#hookremove)

That way you don’t need to expose the [hook()](#hook) method to consumers of your library

### hook()

Invoke before and after hooks. Returns a promise.

```js
hook(nameOrNames, [options,] method)
```

<table>
  <thead>
    <tr>
      <th>Argument</th>
      <th>Type</th>
      <th>Description</th>
      <th>Required</th>
    </tr>
  </thead>
  <tr>
    <th align="left"><code>name</code></th>
    <td>String or Array of Strings</td>
    <td>Hook name, for example <code>'save'</code>. Or an array of names, see example below.</td>
    <td>Yes</td>
  </tr>
  <tr>
    <th align="left"><code>options</code></th>
    <td>Object</td>
    <td>Will be passed to all before hooks as reference, so they can mutate it</td>
    <td>No, defaults to empty object (<code>{}</code>)</td>
  </tr>
  <tr>
    <th align="left"><code>method</code></th>
    <td>Function</td>
    <td>Callback to be executed after all before hooks finished execution successfully. <code>options</code> is passed as first argument</td>
    <td>Yes</td>
  </tr>
</table>

Resolves with whatever `method` returns or resolves with.
Rejects with error that is thrown or rejected with by

1. Any of the before hooks, whichever rejects / throws first
2. `method`
3. Any of the after hooks, whichever rejects / throws first

Simple Example

```js
hook('save', record, function (record) {
  return store.save(record)
})
// shorter:  hook('save', record, store.save)

hook.before('save', function addTimestamps (record) {
  const now = new Date().toISOString()
  if (record.createdAt) {
    record.updatedAt = now
  } else {
    record.createdAt = now
  }
})
```

Example defining multiple hooks at once.

```js
hook(['add', 'save'], record, function (record) {
  return store.save(record)
})

hook.before('add', function addTimestamps (record) {
  if (!record.type) {
    throw new Error('type property is required')
  }
})

hook.before('save', function addTimestamps (record) {
  if (!record.type) {
    throw new Error('type property is required')
  }
})
```

Defining multiple hooks is helpful if you have similar methods for which you want to define separate hooks, but also an additional hook that gets called for all at once. The example above is equal to this:

```js
hook('add', record, function (record) {
  return hook('save', record, function (record) {
    return store.save(record)
  })
})
```

### hook.before()

Add before hook for given name. Returns `hook` instance for chaining.

```js
hook.before(name, method)
```

<table>
  <thead>
    <tr>
      <th>Argument</th>
      <th>Type</th>
      <th>Description</th>
      <th>Required</th>
    </tr>
  </thead>
  <tr>
    <th align="left"><code>name</code></th>
    <td>String</td>
    <td>Hook name, for example <code>'save'</code></td>
    <td>Yes</td>
  </tr>
  <tr>
    <th align="left"><code>method</code></th>
    <td>Function</td>
    <td>
      Executed before the wrapped method. Called with the hook’s
      <code>options</code> argument. Before hooks can mutate the passed options
      before they are passed to the wrapped method.
    </td>
    <td>Yes</td>
  </tr>
</table>

Example

```js
hook.before('save', function validate (record) {
  if (!record.name) {
    throw new Error('name property is required')
  }
})
```

### hook.error()

Add error hook for given name. Returns `hook` instance for chaining.

```js
hook.error(name, method)
```

<table>
  <thead>
    <tr>
      <th>Argument</th>
      <th>Type</th>
      <th>Description</th>
      <th>Required</th>
    </tr>
  </thead>
  <tr>
    <th align="left"><code>name</code></th>
    <td>String</td>
    <td>Hook name, for example <code>'save'</code></td>
    <td>Yes</td>
  </tr>
  <tr>
    <th align="left"><code>method</code></th>
    <td>Function</td>
    <td>
      Executed when an error occurred in either the wrapped method or a
      <code>before</code> hook. Called with the thrown <code>error</code>
      and the hook’s <code>options</code> argument. The first <code>method</code>
      which does not throw an error will set the result that the after hook
      methods will receive.
    </td>
    <td>Yes</td>
  </tr>
</table>

Example

```js
hook.error('save', function (error, options) {
  if (error.ignore) return
  throw error
})
```

### hook.after()

Add after hook for given name. Returns `hook` instance for chaining.

```js
hook.after(name, method)
```

<table>
  <thead>
    <tr>
      <th>Argument</th>
      <th>Type</th>
      <th>Description</th>
      <th>Required</th>
    </tr>
  </thead>
  <tr>
    <th align="left"><code>name</code></th>
    <td>String</td>
    <td>Hook name, for example <code>'save'</code></td>
    <td>Yes</td>
  </tr>
  <tr>
    <th align="left"><code>method</code></th>
    <td>Function</td>
    <td>
    Executed after wrapped method. Called with what the wrapped method
    resolves with the hook’s <code>options</code> argument.
    </td>
    <td>Yes</td>
  </tr>
</table>

Example

```js
hook.after('save', function (result, options) {
  if (result.updatedAt) {
    app.emit('update', result)
  } else {
    app.emit('create', result)
  }
})
```

### hook.wrap()

Add wrap hook for given name. Returns `hook` instance for chaining.

```js
hook.wrap(name, method)
```

<table>
  <thead>
    <tr>
      <th>Argument</th>
      <th>Type</th>
      <th>Description</th>
      <th>Required</th>
    </tr>
  </thead>
  <tr>
    <th align="left"><code>name</code></th>
    <td>String</td>
    <td>Hook name, for example <code>'save'</code></td>
    <td>Yes</td>
  </tr>
  <tr>
    <th align="left"><code>method</code></th>
    <td>Function</td>
    <td>
      Receives both the wrapped method and the passed options as arguments so it can add logic before and after the wrapped method, it can handle errors and even replace the wrapped method altogether
    </td>
    <td>Yes</td>
  </tr>
</table>

Example

```js
hook.wrap('save', async function (saveInDatabase, options) {
  if (!record.name) {
    throw new Error('name property is required')
  }

  try {
    const result = await saveInDatabase(options)

    if (result.updatedAt) {
      app.emit('update', result)
    } else {
      app.emit('create', result)
    }

    return result
  } catch (error) {
    if (error.ignore) return
    throw error
  }
})
```

See also: [Test mock example](examples/test-mock-example.md)

### hook.remove()

Removes hook for given name. Returns `hook` instance for chaining.

```js
hook.remove(name, hookMethod)
```

<table>
  <thead>
    <tr>
      <th>Argument</th>
      <th>Type</th>
      <th>Description</th>
      <th>Required</th>
    </tr>
  </thead>
  <tr>
    <th align="left"><code>name</code></th>
    <td>String</td>
    <td>Hook name, for example <code>'save'</code></td>
    <td>Yes</td>
  </tr>
  <tr>
    <th align="left"><code>beforeHookMethod</code></th>
    <td>Function</td>
    <td>
      Same function that was previously passed to <code>hook.before()</code>, <code>hook.error()</code>, <code>hook.after()</code> or <code>hook.wrap()</code>
    </td>
    <td>Yes</td>
  </tr>
</table>

Example

```js
hook.remove('save', validateRecord)
```

## See also

If `before-after-hook` is not for you, have a look at one of these alternatives:

- https://github.com/keystonejs/grappling-hook
- https://github.com/sebelga/promised-hooks
- https://github.com/bnoguchi/hooks-js
- https://github.com/cb1kenobi/hook-emitter

## License

[Apache 2.0](LICENSE)
