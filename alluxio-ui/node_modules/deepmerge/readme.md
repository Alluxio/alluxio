# deepmerge

Merges the enumerable attributes of two or more objects deeply.

> UMD bundle is 567B minified+gzipped


### Migration from 1.x to 2+

[***Check out the changes from version 1.x to 2.0.0***](https://github.com/KyleAMathews/deepmerge/blob/master/changelog.md#200)

For the legacy array element-merging algorithm, see [the `arrayMerge` option below](#arraymerge).


## Getting Started

### Example Usage
<!--js
var merge = require('./')
-->

```js
var x = {
	foo: { bar: 3 },
	array: [{
		does: 'work',
		too: [ 1, 2, 3 ]
	}]
}

var y = {
	foo: { baz: 4 },
	quux: 5,
	array: [{
		does: 'work',
		too: [ 4, 5, 6 ]
	}, {
		really: 'yes'
	}]
}

var expected = {
	foo: {
		bar: 3,
		baz: 4
	},
	array: [{
		does: 'work',
		too: [ 1, 2, 3 ]
	}, {
		does: 'work',
		too: [ 4, 5, 6 ]
	}, {
		really: 'yes'
	}],
	quux: 5
}

merge(x, y) // => expected
```


### Installation

With [npm](http://npmjs.org) do:

```sh
npm install deepmerge
```

deepmerge can be used directly in the browser without the use of package managers/bundlers as well:  [UMD version from unpkg.com](https://unpkg.com/deepmerge/dist/umd.js).


### Includes

CommonJS:
```
var merge = require('deepmerge')
```

ES Modules:
```
import * as deepmerge from 'deepmerge'
```

(support for `import merge from 'deepmerge'` was removed because of a [Webpack bug](https://github.com/webpack/webpack/issues/6584)).


# API


## `merge(x, y, [options])`

Merge two objects `x` and `y` deeply, returning a new merged object with the
elements from both `x` and `y`.

If an element at the same key is present for both `x` and `y`, the value from
`y` will appear in the result.

Merging creates a new object, so that neither `x` or `y` is modified.

**Note:** By default, arrays are merged by concatenating them.

## `merge.all(arrayOfObjects, [options])`

Merges any number of objects into a single result object.

```js
var x = { foo: { bar: 3 } }
var y = { foo: { baz: 4 } }
var z = { bar: 'yay!' }

var expected = { foo: { bar: 3, baz: 4 }, bar: 'yay!' }

merge.all([x, y, z]) // => expected
```


## Options

### `arrayMerge`

There are multiple ways to merge two arrays, below are a few examples but you can also create your own custom function.

#### Overwrite Array

Overwrites the existing array values completely rather than concatenating them

```js
const overwriteMerge = (destinationArray, sourceArray, options) => sourceArray

merge(
	[1, 2, 3],
	[3, 2, 1],
	{ arrayMerge: overwriteMerge }
) // => [3, 2, 1]
```

#### Combine Array

Combine arrays, such as overwriting existing defaults while also adding/keeping values that are different names

To use the legacy (pre-version-2.0.0) array merging algorithm, use the following:

```js
const emptyTarget = value => Array.isArray(value) ? [] : {}
const clone = (value, options) => merge(emptyTarget(value), value, options)

function combineMerge(target, source, options) {
	const destination = target.slice()

	source.forEach(function(e, i) {
		if (typeof destination[i] === 'undefined') {
			const cloneRequested = options.clone !== false
			const shouldClone = cloneRequested && options.isMergeableObject(e)
			destination[i] = shouldClone ? clone(e, options) : e
		} else if (options.isMergeableObject(e)) {
			destination[i] = merge(target[i], e, options)
		} else if (target.indexOf(e) === -1) {
			destination.push(e)
		}
	})
	return destination
}

merge(
	[{ a: true }],
	[{ b: true }, 'ah yup'],
	{ arrayMerge: combineMerge }
) // => [{ a: true, b: true }, 'ah yup']
```

### `isMergeableObject`

By default, deepmerge clones every property from almost every kind of object.

You may not want this, if your objects are of special types, and you want to copy the whole object instead of just copying its properties.

You can accomplish this by passing in a function for the `isMergeableObject` option.

If you only want to clone properties of plain objects, and ignore all "special" kinds of instantiated objects, you probably want to drop in [`is-plain-object`](https://github.com/jonschlinkert/is-plain-object).

```js
const isPlainObject = require('is-plain-object')

function SuperSpecial() {
	this.special = 'oh yeah man totally'
}

const instantiatedSpecialObject = new SuperSpecial()

const target = {
	someProperty: {
		cool: 'oh for sure'
	}
}

const source = {
	someProperty: instantiatedSpecialObject
}

const defaultOutput = merge(target, source)

defaultOutput.someProperty.cool // => 'oh for sure'
defaultOutput.someProperty.special // => 'oh yeah man totally'
defaultOutput.someProperty instanceof SuperSpecial // => false

const customMergeOutput = merge(target, source, {
	isMergeableObject: isPlainObject
})

customMergeOutput.someProperty.cool // => undefined
customMergeOutput.someProperty.special // => 'oh yeah man totally'
customMergeOutput.someProperty instanceof SuperSpecial // => true
```

### `customMerge`

Specifies a function which can be used to override the default merge behavior for a property, based on the property name.

The `customMerge` function will be passed the key for each property, and should return the function which should be used to merge the values for that property.

It may also return undefined, in which case the default merge behaviour will be used.

```js
const alex = {
    name: {
        first: 'Alex',
        last: 'Alexson'
    },
    pets: ['Cat', 'Parrot']
}

const tony = {
    name: {
        first: 'Tony',
        last: 'Tonison'
    },
    pets: ['Dog']
}

const mergeNames = (nameA, nameB) => `${nameA.first} and ${nameB.first}`

const options = {
    customMerge: (key) => {
        if (key === 'name') {
            return mergeNames
        }
    }
}

const result = merge(alex, tony, options)

result.name // => 'Alex and Tony'
result.pets // => ['Cat', 'Parrot', 'Dog']
```


### `clone`

*Deprecated.*

Defaults to `true`.

If `clone` is `false` then child objects will be copied directly instead of being cloned.  This was the default behavior before version 2.x.


# Testing

With [npm](http://npmjs.org) do:

```sh
npm test
```


# License

MIT
