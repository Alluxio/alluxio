## CHANGELOG

Here are listed the changelogs until 0.8.2, if you are looking for more recent releases changelog please refer to the dedicated GitHub [releases](https://github.com/souporserious/react-popper/releases) page, where you will find all the releases plus the changelog for each of them.


----------

### 0.8.2

fix es5 build [#90](https://github.com/souporserious/react-popper/pull/90)

### 0.8.1

Move back to controlling DOM updates through React

Update & clean dependencies [#89](https://github.com/souporserious/react-popper/pull/89)

### 0.8.0

Upgrade PopperJS dependency to `1.12.9`

Fix `Popper` ref getting called too many times [#81](https://github.com/souporserious/react-popper/issues/81)

Let PopperJS style DOM for better performance as described in [vjeux's talk](https://speakerdeck.com/vjeux/react-rally-animated-react-performance-toolbox).

### 0.7.5

Fix PopperJS instantiation [#77](https://github.com/souporserious/react-popper/pull/77)

### 0.7.4

Allow React 16 as a peerDependency [#59](https://github.com/souporserious/react-popper/pull/59)

Updates TypeScript definition for IPopperChildrenProps [#61](https://github.com/souporserious/react-popper/pull/61)

Made scripts platform independent [#63](https://github.com/souporserious/react-popper/pull/63)

### 0.7.3

Upgraded dependencies [#44](https://github.com/souporserious/react-popper/pull/44)

Fix missing data-x-out-of-boundaries attribute [#45](https://github.com/souporserious/react-popper/pull/45)

Update how react-popper.d.ts imports PopperJS [#51](https://github.com/souporserious/react-popper/pull/51)

### 0.7.2

Fix `top` and `left` arrow calculation. Disregard the note below about changing the CSS positioning, this was an error on `react-popper`'s part.

### 0.7.1

Support `top` and `left` arrow offsets together, be aware this most likely broke any prior CSS positioning your arrows [#37](https://github.com/souporserious/react-popper/pull/37)

Fix `scheduleUpdate` call if `this._popper` does not exist [#38](https://github.com/souporserious/react-popper/pull/38)

Add typescript definitions [#40](https://github.com/souporserious/react-popper/pull/40)

Upgrade to Popper.js 1.10.8

### 0.7.0

Change `Target`, `Popper`, and `Arrow` component's `tag` prop to `component` to allow custom components to be passed in.

Upgrade PopperJS 1.10.2

### 0.6.6

Upgrade PopperJS to 1.9.9

### 0.6.5

Call `innerRef` in _all_ component child functions

### 0.6.4

Call `innerRef` in child function as well

### 0.6.3

Upgrade PopperJS to 1.9.5

### 0.6.2

Replace `lodash.isequal` with `is-equal-shallow`

### 0.6.1

Pass down `scheduleUpdate` to `Popper` child function to allow programatic updates

Upgrade to Popper.js 1.9.4

Fix `modifier.function` is deprecated, use `modifier.fn` [#22](https://github.com/souporserious/react-popper/pull/22)

### 0.6.0

Make sure to pass props from above down to child function, fixes [#13](https://github.com/souporserious/react-popper/issues/13)

Recalculate size of `Popper` when children change, fixes [#15](https://github.com/souporserious/react-popper/issues/15)

### 0.5.0

Use `prop-types` package instead of React PropTypes [#9](https://github.com/souporserious/react-popper/pull/9)

Make updateState modifier return data object [#11](https://github.com/souporserious/react-popper/pull/11)

Removed `findDOMNode` 🎉

Moved back to `tag` instead of `component`. Use a child function now for custom components and pass down the provided ref to the proper component.

Removed default classNames for `popper` and `popper__arrow` so we can be unopinionated about styling.

### 0.4.3

Allow passing children through to components

### 0.4.2

Move back to `translate3d` and round values since half pixel placement was the culprit for causing blurry text

### 0.4.1

Don't use `translate3d` since it causes blurry text on lower res displays

### 0.4.0

Remove `getRef` function since it seems to be causing problems.

Move functional components to classes so we can get nodes more reliably.

Spread modifier styles inside `_getPopperStyle` [#6](https://github.com/souporserious/react-popper/pull/6)

### 0.3.0

Renamed `PopperManager` -> `Manager`

Added `getRef` prop to `Target`, `Popper`, and `Arrow` components

### 0.2.2

Bundle popper.js with dist build

### 0.2.1

Remove React ARIA from demos for now

### 0.2.0

New API see README for full docs

### 0.1.0

Initial release
