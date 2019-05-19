<a href="https://fontawesome.com">
  <img align="right" width="100" height="100" alt="Official Javascript Component" src="https://img.fortawesome.com/349cfdf6/official-javascript-component.svg">
</a>

# react-fontawesome

[![npm](https://img.shields.io/npm/v/@fortawesome/react-fontawesome.svg?style=flat-square)](https://www.npmjs.com/package/@fortawesome/react-fontawesome)

> Font Awesome 5 React component using SVG with JS

<!-- toc -->

- [Introduction](#introduction)
    + [Upgrading Font Awesome?](#upgrading-font-awesome)
    + [Get started](#get-started)
    + [Learn about our new SVG implementation](#learn-about-our-new-svg-implementation)
    + [Going from 0.0.x to 0.1.0](#going-from-00x-to-010)
- [Installation](#installation)
- [Add more styles or Pro icons](#add-more-styles-or-pro-icons)
- [or with Yarn](#or-with-yarn)
- [Usage](#usage)
  * [Explicit Import](#explicit-import)
  * [Build a Library to Reference Icons Throughout Your App More Conveniently](#build-a-library-to-reference-icons-throughout-your-app-more-conveniently)
  * [Unit Testing](#unit-testing)
  * [Processing i elements into svg using Font Awesome](#processing-i-elements-into-svg-using-font-awesome)
- [Features](#features)
  * [Basic](#basic)
  * [Advanced](#advanced)
  * [TypeScript](#typescript)
- [Frequent questions](#frequent-questions)
  * [How do I import the same icon from two different styles?](#how-do-i-import-the-same-icon-from-two-different-styles)
  * [I don't think tree-shaking is working; got any advice?](#i-dont-think-tree-shaking-is-working-got-any-advice)
- [How to Help](#how-to-help)
- [Contributors](#contributors)
- [Releasing this project (only project owners can do this)](#releasing-this-project-only-project-owners-can-do-this)

<!-- tocstop -->

## Introduction

Hey there! We're glad you're here...

#### Upgrading Font Awesome?

If you've used Font Awesome in the past (version 4 or older) there are some
things that you should learn before you dive in.

> https://fontawesome.com/how-to-use/on-the-web/setup/upgrading-from-version-4

#### Get started

This package is for integrating with React. If you aren't using React then it's
not going to help you. Head over to our "Get Started" page for some guidance.

> https://fontawesome.com/how-to-use/on-the-web/setup/getting-started

#### Learn about our new SVG implementation

This package, under the hood, uses SVG with JS and the `@fortawesome/fontawesome-svg-core` library. This implementation differs drastically from
the web fonts implementation that was used in version 4 and older of Font Awesome. You might head over there to learn about how it works.

> https://fontawesome.com/how-to-use/on-the-web/advanced/svg-javascript-core

#### Going from 0.0.x to 0.1.0

See [UPGRADING.md](./UPGRADING.md).

You might also be interested in the larger umbrella project [UPGRADING.md](https://github.com/FortAwesome/Font-Awesome/blob/master/UPGRADING.md)

## Installation

```
$ npm i --save @fortawesome/fontawesome-svg-core
$ npm i --save @fortawesome/free-solid-svg-icons
$ npm i --save @fortawesome/react-fontawesome
```

## Add more styles or Pro icons

Brands are separated into their own style and for customers upgrading from
version 4 to 5 we have a limited number of Regular icons available.

**Visit [fontawesome.com/icons](https://fontawesome.com/icons) to search for free and Pro icons**

```
$ npm i --save @fortawesome/free-brands-svg-icons
$ npm i --save @fortawesome/free-regular-svg-icons
```

If you are a [Font Awesome Pro](https://fontawesome.com/pro) subscriber you can install Pro packages; this requires [additional configuration](https://fontawesome.com/how-to-use/on-the-web/setup/using-package-managers).

```
$ npm i --save @fortawesome/pro-solid-svg-icons
$ npm i --save @fortawesome/pro-regular-svg-icons
$ npm i --save @fortawesome/pro-light-svg-icons
```

## or with Yarn

```
$ yarn add @fortawesome/fontawesome-svg-core
$ yarn add @fortawesome/free-solid-svg-icons
$ yarn add @fortawesome/react-fontawesome
```

## Usage

You can use Font Awesome icons in your React components as simply as this:

```javascript
<FontAwesomeIcon icon="coffee" />
```

That simple usage is made possible when you add the `"coffee"` icon, to the
_library_.

This is one of the two ways you can use Font Awesome 5 with React. We'll
summarize both ways briefly and then get into the details of each below.

1.  **Explicit Import**

    Allows icons to be subsetted, optimizing your final bundle. Only the icons
    you import are included in the bundle. However, explicitly importing icons
    into each of many components in your app might become tedious, so you may
    want to build a library.

2.  **Build a Library**

    Explicitly import icons just once in some init module. Then add them to the
    library. Then reference any of them by icon name as a string from any
    component. No need to import the icons into each component once they're in
    the library.

### Explicit Import

For this example, we'll also reference the `@fortawesome/free-solid-svg-icons`
module, so make sure you've added it to the project as well:

```
$ npm i --save @fortawesome/free-solid-svg-icons
```

or

```
$ yarn add @fortawesome/free-solid-svg-icons
```

Now, a simple React component might look like this:

```javascript
import ReactDOM from 'react-dom'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faCoffee } from '@fortawesome/free-solid-svg-icons'

const element = <FontAwesomeIcon icon={faCoffee} />

ReactDOM.render(element, document.body)
```

Notice that the `faCoffee` icon is imported from
`@fortawesome/free-solid-svg-icons` as an object and then provided to the
`icon` prop as an object.

Explicitly importing icons like this allows us to subset Font Awesome's
thousands of icons to include only those you use in your final bundled file.

### Build a Library to Reference Icons Throughout Your App More Conveniently

You probably want to use our icons in more than one component in your app,
right?

But with explicit importing, it could become tedious to import into each of
your app's components every icon you want to reference in that component.

So, add them to the _library_. Do this setup once in some initializing module
of your app, adding all of the icons you'll use in your app's React components.

Suppose `App.js` initializes my app, including the library. For this example,
we'll add two individual icons, `faCheckSquare` and `faCoffee`. We also add all
of the brands in `@fortawesome/free-brands-svg-icons`. This example would
illustrate the benefits of building a library even more clearly if it involved
fifty or a hundred icons, but we'll keep the example brief and leave it to your
imagination as to how this might scale up with lots of icons.

Don't forget to add `@fortawesome/free-brands-svg-icons`:

```
$ npm i --save @fortawesome/free-brands-svg-icons
```

or

```
$ yarn add @fortawesome/free-brands-svg-icons
```

In `App.js`, where our app is initialized:

```javascript
import ReactDOM from 'react-dom'
import { library } from '@fortawesome/fontawesome-svg-core'
import { fab } from '@fortawesome/free-brands-svg-icons'
import { faCheckSquare, faCoffee } from '@fortawesome/free-solid-svg-icons'

library.add(fab, faCheckSquare, faCoffee)
```

OK, so what's happening here?

In our call to <span style="white-space:nowrap;">`library.add()`</span> we're passing

- `fab`: which represents _all_ of the brand icons in
  <span style="white-space:nowrap;">`@fortawesome/free-brands-svg-icons`</span>.
  So any of the brand icons in that package may be referenced by icon name
  as a string anywhere else in our app.
  For example: `"apple"`, `"microsoft"`, or `"google"`.
- `faCheckSquare` and `faCoffee`: Adding each of these icons individually
  allows us to refer to them throughout our app by their icon string names,
  `"check-square"` and `"coffee"`, respectively.

Now, suppose you also have React components `Beverage` and `Gadget` in your app.
You don't have to re-import your icons into them. Just import the `FontAwesomeIcon`
component, and when you use it, supply the icon prop an icon name as a string.

We'll make `Beverage.js` a functional component:

```javascript
import React from 'react'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'

export const Beverage = () => (
  <div>
    <FontAwesomeIcon icon="check-square" />
    Favorite beverage: <FontAwesomeIcon icon="coffee" />
  </div>
)
```

There's one another piece of magic that's happening in the background when
providing icon names as strings like this: the `fas` prefix (for Font Awesome
Solid) is being inferred as the default. Later, we'll look at what that means
and how we can do something different than the default.

Now suppose `Gadget.js` looks like this:

```javascript
import React from 'react'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'

export const Gadget = () => (
  <div>
    <FontAwesomeIcon icon="check-square" />
    Popular gadgets come from vendors like:
    <FontAwesomeIcon icon={['fab', 'apple']} />
    <FontAwesomeIcon icon={['fab', 'microsoft']} />
    <FontAwesomeIcon icon={['fab', 'google']} />
  </div>
)
```

Notice:

- We used the `"check-square"` icon name again in this component, though we
  didn't have to explicitly import it into this component. With one explicit import of
  that icon in `App.js`, and adding it to the library, we've managed to use
  it by name in multiple components.
- We used the `"apple"`, `"microsoft"`, and `"google"` brand icons, which were
  never explicitly _individually_ imported, but they're available to us by
  name as strings because `fab` was added to our library in `App.js`, and
  `fab` includes all of those icons.
- We added the `fab` prefix to reference those brand icons.

Adding a prefix—and the syntax we used to do it—are new. So what's
going on here?

First, recall when we introduced `<FontAwesomeIcon icon="coffee"/>` and learned
that a prefix of `fas` was being added to `"coffee"` by default.

The `"check-square"` icon is getting a default prefix of `fas` here too, which
is what we want, because that icon also lives in the
`@fortawesome/free-solid-svg-icons` package.

However, the `"apple"`, `"microsoft"`, and `"google"` brand icons live in the
package `@fortawesome/free-brands-svg-icons`. So we need to specify a
different prefix for them—not the default `fas`, but `fab`, for Font Awesome
_Brand_.

When specifying a prefix with an icon name, both are given as strings.

Now, what about that syntax?

The `icon` prop expects a single object:

- It could be an icon object, like `{faCoffee}`.
- It could a string object, like `"coffee"`.
  (The curly braces around a string object supplied to a prop are optional,
  so we've omitted them.)
- Or it could be an `Array` of strings, where the first element is a prefix,
  and the second element is the icon name: `{["fab", "apple"]}`

### Unit Testing

When testing components, you'll want to make sure that any icons referenced in those components are available for testing purposes. You have a couple choices here:

1.  If you want to test a child component on its own, you can [import its icons explicitly](#explicit-import).

2.  If you've built a library instead, and your test doesn't include your root component that defines your library of icons, you may see errors like this:

    `Could not find icon { prefix: 'fas', iconName: 'chevron-right' }`

    If this happens, and the icon isn't important to the particular test, you can mock FontAwesomeIcon like this:

    ```js
    import React from 'react'

    export function FontAwesomeIcon(props) {
      return <i className="fa" />
    }
    ```

    With [create-react-app](https://github.com/facebook/create-react-app), you can put this code in `src/__mocks__/@fortawesome/react-fontawesome.js` to automatically include it in any tests, and alleviate errors.

### Processing i elements into svg using Font Awesome

Our hope and intention is that React users will use this package (`react-fontawesome`)
when using Font Awesome. This component leverages React's architecture and philosophy.

However, **if you cannot use these components everywhere in your app and still
have `<i>` tags on your page that need to be converted to `<svg>` tags we can
still help you**.

A basic installation of [Font Awesome](https://fontawesome.com/how-to-use/on-the-web/setup/getting-started?using=svg-with-js) has
the ability to automatically transform `<i class="fas fa-coffee"></i>` into
`<svg class="...">...</svg>` icons. This technology works with the browser's
DOM, [`requestAnimationFrame`][raf], and [`MutationObserver`][mo].

When using the `@fortawesome/fontawesome-svg-core` package this **behavior is
disabled by default**. (We would _highly_ recommend you use `FontAwesomeIcon`
if you can) This project uses that core package so you will have to explicitly
enable it like this:

To configure the core library to convert non-React'ified parts of your App:

```javascript
import { dom } from '@fortawesome/fontawesome-svg-core'

dom.watch() // This will kick of the initial replacement of i to svg tags and configure a MutationObserver
```

[raf]: https://developer.mozilla.org/en-US/docs/Web/API/window/requestAnimationFrame
[mo]: https://developer.mozilla.org/en-US/docs/Web/API/MutationObserver

## Features

The following features are available as part of Font Awesome. Note that the syntax is different from our general web-use documentation.

In the following code snippets, we'll use the shortcut notation for the
icons—referencing the icons by their names as strings.

But remember, that option is only valid after you've either
explicitly imported and added those icons to the library, or externally
loaded an icon bundle. See the sections above for the details.

### Basic

[Size](https://fontawesome.com/how-to-use/on-the-web/styling/sizing-icons):

```javascript
<FontAwesomeIcon icon="spinner" size="xs" />
<FontAwesomeIcon icon="spinner" size="lg" />
<FontAwesomeIcon icon="spinner" size="6x" />
```

[Fixed width](https://fontawesome.com/how-to-use/on-the-web/styling/fixed-width-icons):

```javascript
<FontAwesomeIcon icon="spinner" fixedWidth />
```

Inverse:

```javascript
<FontAwesomeIcon icon="spinner" inverse />
```

[List Items](https://fontawesome.com/how-to-use/on-the-web/styling/icons-in-a-list):

```javascript
<FontAwesomeIcon icon="spinner" listItem />
```

[Rotate](https://fontawesome.com/how-to-use/on-the-web/styling/rotating-icons):

```javascript
<FontAwesomeIcon icon="spinner" rotation={90} />
<FontAwesomeIcon icon="spinner" rotation={180} />
<FontAwesomeIcon icon="spinner" rotation={270} />
```

Flip horizontally, vertically, or both:

```javascript
<FontAwesomeIcon icon="spinner" flip="horizontal" />
<FontAwesomeIcon icon="spinner" flip="vertical" />
<FontAwesomeIcon icon="spinner" flip="both" />
```

Spin and pulse [animation](https://fontawesome.com/how-to-use/on-the-web/styling/animating-icons):

```javascript
<FontAwesomeIcon icon="spinner" spin />
<FontAwesomeIcon icon="spinner" pulse />
```

[Border](https://fontawesome.com/how-to-use/on-the-web/styling/bordered-pulled-icons):

```javascript
<FontAwesomeIcon icon="spinner" border />
```

[Pull left or right](https://fontawesome.com/how-to-use/on-the-web/styling/bordered-pulled-icons):

```javascript
<FontAwesomeIcon icon="spinner" pull="left" />
<FontAwesomeIcon icon="spinner" pull="right" />
```

Your own class names:

```javascript
<FontAwesomeIcon icon="spinner" className="highlight" />
```

### Advanced

[Power Transforms](https://fontawesome.com/how-to-use/on-the-web/styling/power-transforms):

```javascript
<FontAwesomeIcon icon="spinner" transform="shrink-6 left-4" />
<FontAwesomeIcon icon="spinner" transform={{ rotate: 42 }} />
```

[Masking](https://fontawesome.com/how-to-use/on-the-web/styling/masking):

```javascript
<FontAwesomeIcon icon="coffee" mask={['far', 'circle']} />
```

[Symbols](https://fontawesome.com/how-to-use/on-the-web/advanced/svg-symbols):

```javascript
<FontAwesomeIcon icon="edit" symbol />
<FontAwesomeIcon icon="edit" symbol="edit-icon" />
```

[Layers](https://fontawesome.com/how-to-use/on-the-web/styling/layering):

```javascript
<span className="fa-layers fa-fw">
  <FontAwesomeIcon icon="square" color="green" />
  <FontAwesomeIcon icon="check" inverse transform="shrink-6" />
</span>
```

### TypeScript

Typings are included in this package. However, most types are defined in the
underlying API library, `@fortawesome/fontawesome-svg-core`.

Suppose that in one module, you add all `fas` icons to the library:

```typescript
import { library } from '@fortawesome/fontawesome-svg-core'
import { fas } from '@fortawesome/free-solid-svg-icons'

library.add(fas)

// ...
```

Then suppose that in another module, you have some code that looks up
one of the icons in the library. The `import` statement below imports two types
and one function:

```typescript
import {
  IconLookup,
  IconDefinition,
  findIconDefinition
} from '@fortawesome/fontawesome-svg-core'

const coffeeLookup: IconLookup = { prefix: 'fas', iconName: 'coffee' }
const coffeeIconDefinition: IconDefinition = findIconDefinition(coffeeLookup)

// ...

export class App extends React.Component {
  render() {
    return (
      <div className="App">
        <h1>
          <FontAwesomeIcon icon={coffeeIconDefinition} />
        </h1>
      </div>
    )
  }
}
```

NOTE: You wouldn't normally declare intermediate objects like `coffeeLookup`
just to look up an icon. So this is cumbersome and needlessly verbose for such
a simple example. The purpose here is just to show how you might import type
definitions and use them in declarations when it _does_ make sense to do so.

Several types, including `IconLookup` and `IconDefinition`, appearing above,
actually originate from the `@fortawesome/fontawesome-common-types` package.
They are re-exported from both `@fortawesome/fontawesome-svg-core` and
`@fortawesome/free-solid-svg-icons` (and other icon packs). This is just to
make importing more convenient in some cases. Refer to the `index.d.ts` in any
module to see which types it exports.

## Frequent questions

### How do I import the same icon from two different styles?

With ES modules and `import` statements we can rename:

```javascript
import { library } from '@fortawesome/fontawesome-svg-core'
import { faStroopwafel as fasFaStroopwafel } from '@fortawesome/pro-solid-svg-icons'
import { faStroopwafel as farFaStroopwafel } from '@fortawesome/pro-regular-svg-icons'

library.add(fasFaStroopwafel, farFaStroopwafel)
```

### I don't think tree-shaking is working; got any advice?

Check out our [docs here](https://fontawesome.com/how-to-use/with-the-api/other/tree-shaking).

## How to Help

Review the following docs before diving in:

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

And then:

1.  Check the existing issue and see if you can help!

## Contributors

The following contributors have either hepled to start this project, have contributed
code, are actively maintaining it (including documentation), or in other ways
being awesome contributors to this project. **We'd like to take a moment to recognize them.**

|                                                            | Name           | GitHub                                             |
| :--------------------------------------------------------: | -------------- | -------------------------------------------------- |
| <img src="https://github.com/NateRadebaugh.png?size=72" /> | Nate Radebaugh | [@NateRadebaugh](https://github.com/NateRadebaugh) |
|   <img src="https://github.com/kirkbross.png?size=72" />   | Kirk Ross      | [@kirkbross](https://github.com/kirkbross)         |
|                                                            | Prateek Goel   | [@prateekgoel](https://github.com/prateekgoel)     |
|    <img src="https://github.com/naortor.png?size=72" />    | Naor Torgeman  | [@naortor](https://github.com/naortor)             |
|   <img src="https://github.com/mmhand123.png?size=72" />   | Matthew Hand   | [@mmhand123](https://github.com/mmhand123)         |
|    <img src="https://github.com/calvinf.png?size=72" />    | calvinf        | [@calvinf](https://github.com/calvinf)             |

If we've missed someone (which is quite likely) submit a Pull Request to us and we'll get it resolved.

The Font Awesome team:

|                                                            | Name           | GitHub                                             |
| :--------------------------------------------------------: | -------------- | -------------------------------------------------- |
| <img src="https://github.com/supercodepoet.png?size=72" /> | Travis Chase   | [@supercodepoet](https://github.com/supercodepoet) |
|   <img src="https://github.com/robmadole.png?size=72" />   | Rob Madole     | [@robmadole](https://github.com/robmadole)         |
|  <img src="https://github.com/mlwilkerson.png?size=72" />  | Mike Wilkerson | [@mlwilkerson](https://github.com/mlwilkerson)     |
|     <img src="https://github.com/talbs.png?size=72" />     | Brian Talbot   | [@talbs](https://github.com/talbs)                 |

## Releasing this project (only project owners can do this)

See [DEVELOPMENT.md](DEVELOPMENT.md#release)
