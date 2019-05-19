## React Popper

[![npm version](https://img.shields.io/npm/v/react-popper.svg)](https://www.npmjs.com/package/react-popper)
[![npm downloads](https://img.shields.io/npm/dm/react-popper.svg)](https://www.npmjs.com/package/react-popper)
[![Dependency Status](https://david-dm.org/souporserious/react-popper.svg)](https://david-dm.org/souporserious/react-popper)
[![JavaScript Style Guide](https://img.shields.io/badge/code_style-standard-brightgreen.svg)](https://standardjs.com)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg)](https://github.com/prettier/prettier)

React wrapper around [PopperJS](https://github.com/FezVrasta/popper.js/).

## Install

`npm install react-popper --save` or `yarn add react-popper`


### CDN

If you prefer to include React Popper globally by marking `react-popper` as external in your application, the `react-popper` library (exposed as `ReactPopper`) provides various single-file distributions, which are hosted on the following CDNs:

* [**cdnjs**](https://cdnjs.com/libraries/react-popper)
```html
<script src="https://cdnjs.cloudflare.com/ajax/libs/react-popper/0.10.2/umd/react-popper.min.js"></script>
```

* [**unpkg**](https://unpkg.com/react-popper/)
```html
<script src="https://unpkg.com/react-popper@0.10.2/dist/umd/react-popper.min.js"></script>
```

> **Note**: To load a specific version of React Popper replace `0.10.2` with the version number.

## Usage

```js
import { Manager, Target, Popper, Arrow } from 'react-popper'

const PopperExample = () => (
  <Manager>
    <Target style={{ width: 120, height: 120, background: '#b4da55' }}>
      Target Box
    </Target>
    <Popper placement="left" className="popper">
      Left Content
      <Arrow className="popper__arrow"/>
    </Popper>
    <Popper placement="right" className="popper">
      Right Content
      <Arrow className="popper__arrow"/>
    </Popper>
  </Manager>
)
```

## Usage with child function

This is a useful way to interact with custom components. Just make sure you pass down the refs properly.

```js
import { Manager, Target, Popper, Arrow } from 'react-popper'

const PopperExample = () => (
  <Manager>
    <Target>
      {({ targetProps }) => (
        <div {...targetProps}>
          Target Box
        </div>
      )}
    </Target>
    <Popper placement="left">
      {({ popperProps, restProps }) => (
        <div
          className="popper"
          {...popperProps}
        >
          Popper Content
          <Arrow>
            {({ arrowProps, restProps }) => (
              <span
                className="popper__arrow"
                {...arrowProps}
              />
            )}
          </Arrow>
        </div>
      )}
    </Popper>
  </Manager>
)
```

## Usage without Manager

It's generally easiest to let the `Manager` and `Target` components handle passing the target DOM element to the `Popper` component. However, you can pass a target [Element](https://developer.mozilla.org/en-US/docs/Web/API/Element) or a [referenceObject](https://popper.js.org/popper-documentation.html#referenceObject) directly into `Popper` if you need to.

Handling DOM Elements from React can be complicated. The `Manager` and `Target` components handle these complexities for you, so their use is strongly recommended when using DOM Elements.

```js
import { PureComonent } from 'react'
import { Popper, Arrow } from 'react-popper'

class StandaloneExample extends PureComponent {
  state = {
    isOpen: false,
  }

  handleClick() = () => {
    this.setState(prevState => ({
      isOpen: !prevState.isOpen
    }))
  }

  render() {
    return (
      <div>
        <div
          ref={(div) => this.target = div}
          style={{ width: 120, height: 120, background: '#b4da55' }}
          onClick={this.handleClick}
        >
          Click {this.state.isOpen ? 'to hide' : 'to show'} popper
        </div>
        {this.state.isOpen && (
          <Popper className="popper" target={this.target}>
            Popper Content
            <Arrow className="popper__arrow"/>
          </Popper>
        )}
      </div>
    )
  }
}
```

## `Shared Props`

`Target`, `Popper`, and `Arrow` all share the following props

#### `component`: PropTypes.oneOfType([PropTypes.node, PropTypes.func])

A valid DOM tag or custom component to render. If using a custom component, an `innerRef` prop will be passed down that **must** be attached to the child component ref.

#### `innerRef`: PropTypes.func

Use this prop to access the internal ref. Does not apply to the `Manager` component since we do not interact with its ref.

## `Manager`

This is a special component that provides the `Target` component to the `Popper` component. Pass any props as you normally would here.

#### `tag`: PropTypes.oneOfType([PropTypes.string, PropTypes.bool])

A valid DOM tag to render. Allows rendering just children by passing `false`. Once React 16 is out, this prop will most likely go away since we will be able to return an array and all this currently does is subscribe `Target` and `Popper`.

## `Target`

This is just a simple component that subscribes to `PopperManager`, so `Popper` can make use of it. Again, pass any props as you normally would here.

Each `Target` must be wrapped in a `Manager`, and each `Manager` can wrap only one `Target`.

#### `children`: PropTypes.oneOfType([PropTypes.node, PropTypes.func])

A `Target`'s child may be one of the following:

- a React element[s]
- a function accepting the following object (all props must be passed down in order for the PopperJS to work properly)

  ```js
  {
    targetProps: {
      ref, // a function that accepts the target component as an argument
    },
    restProps, // any other props that came through the Target component
  }
  ```


## `Popper`

Your popper that gets attached to the `Target` component.

Each `Popper` must either be wrapped in a `Manager`, or passed a `target` prop directly. Each `Manager` can wrap multiple `Popper` components.

#### `placement`: PropTypes.oneOf(Popper.placements)
#### `eventsEnabled`: PropTypes.bool
#### `modifiers`: PropTypes.object
#### `target`: PropTypes.oneOfType([PropTypes.instanceOf(Element), Popper.referenceObject])

Passes respective options to a new [Popper instance](https://github.com/FezVrasta/popper.js/blob/master/docs/_includes/popper-documentation.md#new-popperreference-popper-options). As for `onCreate` and `onUpdate`, these callbacks were intentionally left out in favor of using the [component lifecycle methods](https://facebook.github.io/react/docs/react-component.html#the-component-lifecycle). If you have a good use case for these please feel free to file and issue and I will consider adding them in.

#### `children`: PropTypes.oneOfType([PropTypes.node, PropTypes.func])

A `Popper`'s child may be one of the following:

- a React element[s]
- a function accepting the following object (all props must be passed down in order for the PopperJS to work properly)

  ```js
  {
    popperProps: {
      ref, // a function that accepts the popper component as an argument
      style, // the styles to apply to the popper element
      'data-placement', // the placement of the Popper
    },
    restProps, // any other props that came through the Popper component
  }
  ```

## `Arrow`

Another component that subscribes to the `Popper` component as an [arrow modifier](https://github.com/FezVrasta/popper.js/blob/master/docs/_includes/popper-documentation.md#modifiers..arrow). Must be a child of `Popper`.

#### `children`: PropTypes.oneOfType([PropTypes.node, PropTypes.func])

An `Arrow`'s child may be one of the following:

- a React element[s]
- a function accepting the following object (all props must be passed down in order for the PopperJS to work properly)

  ```js
  {
    arrowProps: {
      ref, // a function that accepts the arrow component as an argument
      style, // the styles to apply to the arrow element
    },
    restProps, // any other props that came through the Arrow component
  }
  ```


## Running Locally

#### clone repo

`git clone git@github.com:souporserious/react-popper.git`

#### move into folder

`cd ~/react-popper`

#### install dependencies

`npm install` or `yarn`

#### run dev mode

`npm run dev` or `yarn dev`

#### open your browser and visit:
`http://localhost:8080/`
