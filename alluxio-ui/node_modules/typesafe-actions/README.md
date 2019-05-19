# typesafe-actions

Typesafe utilities for "action-creators" in Redux / Flux Architecture.

Featuring flexible functional API that's specifically designed to reduce types **verbosity**
and **complexity** (explained in [motivation](#motivation)).

[![Latest Stable Version](https://img.shields.io/npm/v/typesafe-actions.svg)](https://www.npmjs.com/package/typesafe-actions)
[![Build Status](https://semaphoreci.com/api/v1/piotrekwitek/typesafe-actions/branches/master/shields_badge.svg)](https://semaphoreci.com/piotrekwitek/typesafe-actions)
[![dependencies Status](https://david-dm.org/piotrwitek/typesafe-actions/status.svg)](https://david-dm.org/piotrwitek/typesafe-actions)
[![peerDependencies Status](https://david-dm.org/piotrwitek/typesafe-actions/peer-status.svg)](https://david-dm.org/piotrwitek/typesafe-actions?type=peer)
[![License](https://img.shields.io/npm/l/typesafe-actions.svg?style=flat)](https://david-dm.org/piotrwitek/typesafe-actions?type=peer)

[![NPM Downloads](https://img.shields.io/npm/dm/typesafe-actions.svg)](https://www.npmjs.com/package/typesafe-actions)
[![NPM Downloads](https://img.shields.io/npm/dt/typesafe-actions.svg)](https://www.npmjs.com/package/typesafe-actions)
[![Bundlephobia Size](https://img.shields.io/bundlephobia/minzip/typesafe-actions.svg)](https://www.npmjs.com/package/typesafe-actions)

> #### :star: _Found it useful? Want more updates?_ [**Show your support by giving a :star:**](https://github.com/piotrwitek/typesafe-actions/stargazers)  

> :book: _This lib is an integral part of [React & Redux TypeScript Guide](https://github.com/piotrwitek/react-redux-typescript-guide)_ :book:  

> :computer: _Reference implementation of Todo-App with `typesafe-actions` on [CodeSandbox](https://codesandbox.io/s/github/piotrwitek/typesafe-actions/tree/master/codesandbox)_ :computer:  

> :tada: _Now updated to be compatible with **TypeScript v3.2.2**_ :tada:  

### Features

* __minimalistic__ - according to `rollup-plugin-filesize` (Bundle size: 2.6 KB, Gzipped size: 808 B) check also on [bundlephobia](https://bundlephobia.com/result?p=typesafe-actions)
* __secure and optimized__ - no external dependencies with 3 different bundle types (`cjs`, `esm` and `umd` for browser)
* __focus on quality__ - complete test-suite for an entire API surface with runtime tests and extra type-tests to guarantee **type soundness**

## Bug Report CodeSandbox
You can use this CodeSandbox to reproduce bug reports: https://codesandbox.io/s/github/piotrwitek/typesafe-actions/tree/master/codesandbox

## Contributing Guide
We are open for contributions. If you're planning to contribute please make sure to read the contributing guide: [CONTRIBUTING.md](/CONTRIBUTING.md)

## Sponsor
**Typesafe-Actions** is an independent open-source project created by people investing their free time for the benefit of our community.

If you are using **Typesafe-Actions** please consider donating as this will guarantee the project will be updated and maintained in the long run.

Issues can be funded by anyone and the money will be transparently distributed to the contributors handling a particular issue.

[![Let's fund issues in this repository](https://issuehunt.io/static/embed/issuehunt-button-v1.svg)](https://issuehunt.io/repos/110746954)

---

## Table of Contents

* [Installation](#installation)
* [Compatibility Notes](#compatibility-notes)
* [Motivation](#motivation)
* [Behold the Mighty "Tutorial"](#behold-the-mighty-tutorial)
* [API Docs](#api-docs)
  * Type-helpers
    * [`ActionType`](#actiontype)
    * [`StateType`](#statetype)
  * Action-creators
    * [`createAction`](#createaction)
    * [`createStandardAction`](#createstandardaction)
    * [`createCustomAction`](#createcustomaction)
    * [`createAsyncAction`](#createasyncaction)
  * Action-helpers
    * [`getType`](#gettype)
    * [`isActionOf`](#isactionof)
    * [`isOfType`](#isoftype)
  * Action-factory
    * [`action`](#action)
* [Migration Guides](#migration-guides)
  * [v1.x.x to v2.x.x](#v1xx-to-v2xx)
  * [Migrating from redux-actions](#migrating-from-redux-actions)
* [Compare to others](#compare-to-others)
  * [redux-actions](#redux-actions)

---

## Installation

```bash
// NPM
npm install typesafe-actions

// YARN
yarn add typesafe-actions
```

[⇧ back to top](#table-of-contents)

---

## Compatibility Notes

### TypeScript support
* `typesafe-actions@1.X.X` - minimal TS v2.7
* `typesafe-actions@2.X.X` - minimal TS v2.9
* `typesafe-actions@3.X.X` - minimal TS v3.2

### Browser Polyfills
If you support older browsers (e.g. IE < 11) and mobile devices please provide this polyfill:
- [Object.assign](https://developer.mozilla.org/pl/docs/Web/JavaScript/Referencje/Obiekty/Object/assign#Polyfill)

You could also consider including a global polyfill in your bundled application, such as core-js or babel-polyfill.
You can check the `React` guidelines on how to do that, specifically: https://reactjs.org/docs/javascript-environment-requirements.html

[⇧ back to top](#table-of-contents)

---

## Motivation

When I started to combine Redux with TypeScript, I was trying to use [redux-actions](https://redux-actions.js.org/) to reduce the maintainability cost and boilerplate of **action-creators**. Unfortunately, the results were intimidating: incorrect type signatures and broken type-inference cascading throughout the entire code-base [(click here for a detailed comparison)](#redux-actions).

Existing solutions in the wild have been either **too verbose because of redundant type annotations** (hard to maintain) or **used classes** (hinders readability and requires using the **new** keyword 😱)

**So I created `typesafe-actions` to address all of the above pain points.**

The core idea was to design an API that would mostly use the power of TypeScript **type-inference** 💪 to lift the "maintainability burden" of type annotations. In addition, I wanted to make it "look and feel" as close as possible to the idiomatic JavaScript ❤️ , so we don't have to write the redundant type annotations that which will create additional noise in your code.

[⇧ back to top](#table-of-contents)

---

## Behold the Mighty "Tutorial"

To showcase the flexibility and the power of the **type-safety** provided by this library, let's build the most common parts of a typical todo-app using a Redux architecture:

> **WARNING:** Please make sure that you understand the following concepts of general programming languages to be able to follow along: [Type Inference](https://www.typescriptlang.org/docs/handbook/type-inference.html), [Control flow analysis](https://github.com/Microsoft/TypeScript/wiki/What%27s-new-in-TypeScript#control-flow-based-type-analysis), [Tagged union types](https://github.com/Microsoft/TypeScript/wiki/What%27s-new-in-TypeScript#tagged-union-types), [Generics](https://www.typescriptlang.org/docs/handbook/generics.html) and some [Advanced Types](https://www.typescriptlang.org/docs/handbook/advanced-types.html).

[⇧ back to top](#table-of-contents)

### - The Actions

Different projects have different needs, and conventions vary across teams, and this is why `typesafe-actions` was designed and built with flexibility in mind. It provides three different factory functions so you can choose whichever would be the best fit for your project.

> **PRO-TIP: string constants limitation in TypeScript** - when using *string constants* as action `type` property, please make sure to only use **const string literals**, because **dynamic string operations** (like string concatenation, template strings, an object used as a dictionary, etc.) will widen the literal type to its super-type, `string`. This will break contextual typing in reducer cases.

```ts
// Example file: './constants.ts'

// WARNING: Incorrect usage
export const ADD = prefix + 'ADD'; // => string
export const ADD = `${prefix}/ADD`; // => string
export default {
   ADD: '@prefix/ADD', // => string
}

// Correct usage
export const ADD = '@prefix/ADD'; // => '@prefix/ADD'
export const TOGGLE = '@prefix/TOGGLE'; // => '@prefix/TOGGLE'
```

#### 1. Classic JS style with constants FTW!
Using this simple function, we'll have complete type-safety with minimal type declaration effort, but we're constrained to use constants (as in regular JS applications), because some of the advanced **action-helpers** (`getType`, `isActionOf`) will not work with such an action-creator. This is still a very compelling option, especially for refactoring existing projects.

```ts
import { action } from 'typesafe-actions';

import { ADD, TOGGLE } from './constants';

export const toggle = (id: string) => action(TOGGLE, id);
// (id: string) => { type: 'todos/TOGGLE'; payload: string; }

export const add = (title: string) => action(ADD, { title, id: cuid(), completed: false } as Todo);
// (title: string) => { type: 'todos/ADD'; payload: Todo; }
```

#### 2. Opinionated without need for constants
This approach will give us access to all the **action-helpers** and eliminate the use of constants, but it's opinionated and will always accept two arguments (1st is payload, and 2nd is meta). It also has a very useful `map` chain method for extra flexibility.
```ts
import { createStandardAction } from 'typesafe-actions';

export const toggle = createStandardAction('todos/TOGGLE')<string>();
// (payload: string) => { type: 'todos/TOGGLE'; payload: string; }

export const add = createStandardAction(ADD).map(
  ({ title }: { title: string }) => ({
    payload: { title, id: cuid(), completed: false } as Todo,
  })
);
// ({ title: string }) => { type: 'todos/ADD'; payload: Todo; }
```
#### 3. Most flexible with all helpers
This approach will give us the best of both worlds: all the **action-helpers** will work and we have the flexibility of providing a variadic amount of named parameters as with regular functions.
```ts
import { createAction } from 'typesafe-actions';

export const toggle = createAction('todos/TOGGLE', resolve => {
  return (id: string) => resolve(id);
});
// (id: string) => { type: 'todos/TOGGLE'; payload: string; }

export const add = createAction('todos/ADD', resolve => {
  return (title: string) => resolve({ title, id: cuid(), completed: false } as Todo);
});
// (title: string) => { type: 'todos/ADD'; payload: Todo; }
```

> For more examples check the [API Docs](#table-of-contents), there are plenty!

[⇧ back to top](#table-of-contents)

### - The Reducer

Here we'll start by generating a **tagged union type** of actions (TodosAction). It's very easy to do using TS type-inference and `ActionType` utility-type provided by `typesafe-actions`.
```ts
import { ActionType, getType } from 'typesafe-actions';

import * as todos from './actions';
export type TodosAction = ActionType<typeof todos>;
```
Now we define a regular reducer function by annotating `state` and `action` arguments with their respective types (use `TodosAction` for action type)
```ts
export default (state: Todo[] = [], action: TodosAction) => {
```
With static types in place we can finally leverage **tagged union types**. Using switch-cases on the common `type` property of action, we can distinguish and narrow the type of `TodosAction` to a single specific action type valid for the corresponding code block.
```ts
  switch (action.type) {
    case getType(todos.add):
      // below action type is narrowed to: { type: "todos/ADD"; payload: Todo; }
      return [...state, action.payload];
    ...
```
Notice we are using `getType` action-helper with the respective action-creator as an argument for the corresponding switch case. This will help to reduce boilerplate and completely remove the need to use **type-constants** in our application.
> Note: If your team prefers to use regular "type constants", no problem. You can still use them with `typesafe-actions`.

> **PRO-TIP:** I recommend to create a `RootAction` in the central point of your redux store - it will model a complete representation of all possible action types in your application. You can even merge it with existing third-party declarations as shown below.
```ts
// types.d.ts
// example of including `react-router` actions in `RootAction`
import { RouterAction, LocationChangeAction } from 'react-router-redux';
type ReactRouterAction = RouterAction | LocationChangeAction;
import { TodosAction } from '../features/todos';

export type RootAction =
  | ReactRouterAction
  | TodosAction;
```

[⇧ back to top](#table-of-contents)

### - The Async-Flow

#### Starring `redux-observable` epics

To handle async-flow of request to a remote resources, we'll implement an `epic`. The `epic` will call a remote API using an injected `todosApi` client and then return a Promise.
To help us simplify the creation process of necessary action-creators, we'll use `createAsyncAction` function providing us with a nice common interface `{ request: ... , success: ... , failure: ... }` that will nicely fit with the functional API of `RxJS`.
This will mitigate **redux verbosity** and greatly reduce the maintenance cost of type annotations for actions objects and action-creators that would otherwise be written by hand.

```ts
// actions.ts
import { createAsyncAction } from 'typesafe-actions';

const fetchTodos = createAsyncAction(
  'FETCH_TODOS_REQUEST',
  'FETCH_TODOS_SUCCESS',
  'FETCH_TODOS_FAILURE'
)<string, Todo[], Error>();

// epics.ts
import { fetchTodos } from './actions';

const fetchTodosFlow: Epic<RootAction, RootAction, RootState, Services> = (action$, store, { todosApi }) =>
  action$.pipe(
    filter(isActionOf(fetchTodos.request)),
    switchMap(action =>
      from(todosApi.getAll(action.payload)).pipe(
        map(fetchTodos.success),
        catchError(pipe(fetchTodos.failure, of))
      )
    );
```

#### Starring `redux-saga` sagas
With sagas it's not possible to achieve the same degree of type-safety as with epics because of limitations coming from `redux-saga` API design.

Typescript issues:
- [Typescript does not currently infer types resulting from a `yield` statement](https://github.com/Microsoft/TypeScript/issues/2983) so you have to manually assert the type  e.g. `const response: Todo[] = yield call(...`

*Here is the latest recommendation although it's not fully optimal. If you cooked something better, please open an issue so we can share it with the world.*

```ts
import { createAsyncAction } from 'typesafe-actions';

const fetchTodos = createAsyncAction(
  'FETCH_TODOS_REQUEST',
  'FETCH_TODOS_SUCCESS',
  'FETCH_TODOS_FAILURE'
)<string, Todo[], Error>();

function* addTodoSaga(action: ReturnType<typeof fetchTodos.request>): Generator {
    const response: Todo[] = yield call(todosApi.getAll, action.payload);

    yield put(fetchTodos.success(response));
  } catch (err) {
    yield put(fetchTodos.failure(err));
  }
}
```

[⇧ back to top](#table-of-contents)

### - The Action-Helpers

I wan't to show you why **action-helpers** was created and why they are useful, so let me guide you trough simple use-case. We're going to implement a side-effect responsible for showing a notification when the user adds a new todo.

#### with action-creators

When you're using only **action-creators** (no type-constants) there is a (`isActionOf`) helper that will accept action-creator as parameter. It is used to filter actions that was created with matching creator. This helper is working like a **type-guard** so it'll narrow **tagged union type** (`RootAction`) containing all the action types to a single specific action type.

```ts
// epics.ts
import { isActionOf } from 'typesafe-actions';

import { add } from './actions';

const addTodoToast: Epic<RootAction, RootAction, RootState, Services> = (action$, store, { toastService }) =>
  action$.pipe(
    filter(isActionOf(add)),
    tap(action => { // here action type is narrowed to: { type: "todos/ADD"; payload: Todo; }
      toastService.success(...);
    })
    ...
    
  // Works with multiple actions! (with type-safety up to 5)
  action$.pipe(
    filter(isActionOf([add, toggle])) // here action type is narrowed to a smaller union:
    // { type: "todos/ADD"; payload: Todo; } | { type: "todos/TOGGLE"; payload: string; }
```

#### with type-constants
Alternatively if your team prefers to use **type-constants**, we have an equivalent helper (`isOfType`) which will accept **type-constants** instead of action-creators and have exact same behavior.
```ts
// epics.ts
import { isOfType } from 'typesafe-actions';

import { ADD } from './constants';

const addTodoToast: Epic<RootAction, RootAction, RootState, Services> = (action$, store, { toastService }) =>
  action$.pipe(
    filter(isTypeOf(ADD)),
    tap(action => { // here action type is narrowed to: { type: "todos/ADD"; payload: Todo; }
    ...
    
  // Works with multiple actions! (with type-safety up to 5)
  action$.pipe(
    filter(isTypeOf([ADD, TOGGLE])) // here action type is narrowed to a smaller union:
    // { type: "todos/ADD"; payload: Todo; } | { type: "todos/TOGGLE"; payload: string; }
```

> **PRO-TIP:** Both helpers above prove useful in all types of conditional statements.
```ts
import { isActionOf, isOfType } from 'typesafe-actions';

if (isActionOf(actions.add, action)) {
  // here action is narrowed to: { type: "todos/ADD"; payload: Todo; }
}
// or with type constants
if (isOfType(types.ADD, action)) {
  // here action is narrowed to: { type: "todos/ADD"; payload: Todo; }
}
```

[⇧ back to top](#table-of-contents)

---

## API Docs

### Type-helpers
Below helper functions are very flexible generalizations, works great with nested structures and will cover numerous different use-cases.

#### ActionType

_Powerful type-helper that will infer union type from **import * as ...** or **action-creator map** object._

```ts
import { ActionType } from 'typesafe-actions';

// with "import * as ..."
import * as todos from './actions';
export type TodosAction = ActionType<typeof todos>;
// TodosAction: { type: 'action1' } | { type: 'action2' } | { type: 'action3' }

// with nested action-creator map case
const actions = {
  action1: createAction('action1'),
  nested: {
    action2: createAction('action2'),
    moreNested: {
      action3: createAction('action3'),
    },
  },
};
export type RootAction = ActionType<typeof actions>;
// RootAction: { type: 'action1' } | { type: 'action2' } | { type: 'action3' }
```

[⇧ back to top](#table-of-contents)

---

#### StateType

_Powerful type helper that will infer state object type from **reducer function** and **nested/combined reducers**._

_**Redux compatibility**: working with redux@4+ types_

```ts
import { combineReducers } from 'redux';
import { StateType } from 'typesafe-actions';

// with reducer function
const todosReducer = (state: Todo[] = [], action: TodosAction) => {
  switch (action.type) {
    case getType(todos.add):
      return [...state, action.payload];
    ...
export type TodosState = StateType<typeof todosReducer>;

// with nested/combined reducers
const rootReducer = combineReducers({
  router: routerReducer,
  counters: countersReducer,
});
export type RootState = StateType<typeof rootReducer>;
```

[⇧ back to top](#table-of-contents)

---

### Action-creators

#### createAction

_Create an enhanced action-creator with unlimited number of arguments._
- Arguments of resulting action-creator will preserve their original semantic names `(id, firstName, lastName)`.
- Returned action objects have predefined properties `({ payload, meta })`

```ts
createAction(type)
createAction(type, actionCallback => {
  return (namedArg1, namedArg2, ...namedArgN) => actionCallback(payload?, meta?)
})
```

Examples:
[> Advanced Usage Examples](src/create-action.spec.ts)

```ts
import { createAction } from 'typesafe-actions';

// Using action callback
// - with type only
const increment = createAction('INCREMENT');
increment(); // { type: 'INCREMENT' };

// - with type and payload
const add = createAction('ADD', action => {
  return (amount: number) => action(amount);
});
add(10); // { type: 'ADD', payload: number }

// - with type and meta
const getTodos = createAction('GET_TODOS', action => {
  return (params: Params) => action(undefined, params);
});
getTodos('some_meta'); // { type: 'GET_TODOS', meta: Params }

// - and finally with type, payload and meta
const getTodo = createAction('GET_TODO', action => {
  return (id: string, meta: string) => action(id, meta);
});
getTodo('some_id', 'some_meta'); // { type: 'GET_TODO', payload: string, meta: string }
```

[⇧ back to top](#table-of-contents)

---

#### createStandardAction

_Create an enhanced action-creator compatible with [Flux Standard Action](https://github.com/redux-utilities/flux-standard-action) to reduce boilerplate and enforce convention._
- Arguments of resulting action-creator are predefined `(payload, meta)`
- Returned action objects have predefined properties `({ payload, meta })`
- But it also contains a `.map()` method that allow to map `(payload, meta)` arguments to a custom action object `({ customProp1, customProp2, ...customPropN })`

```ts
createStandardAction(type)()
createStandardAction(type)<TPayload | void, TMeta?>()
createStandardAction(type).map((payload, meta) => ({ customProp1, customProp2, ...customPropN }))
```

Examples:
[> Advanced Usage Examples](src/create-standard-action.spec.ts)

```ts
import { createStandardAction } from 'typesafe-actions';

// Very concise with use of generic type arguments
// - with type only
const increment = createStandardAction('INCREMENT')();
const increment = createStandardAction('INCREMENT')<void>();
increment(); // { type: 'INCREMENT' }

// - with type and payload
const add = createStandardAction('ADD')<number>();
add(10); // { type: 'INCREMENT', payload: number }

// - with type and meta
const getData = createStandardAction('GET_DATA')<void, string>();
getData(undefined, 'meta'); // { type: 'INCREMENT', meta: string }

// - and finally with type, payload and meta
const getData = createStandardAction('GET_DATA')<number, string>();
getData(1, 'meta'); // { type: 'INCREMENT', payload: number, meta: string }

// Can map payload and meta arguments to a custom action object
const notify = createStandardAction('NOTIFY').map(
  (payload: string, meta: Meta) => ({
    from: meta.username,
    message: `${username}: ${payload}`,
    messageType: meta.type,
    datetime: new Date(),
  })
);

notify('Hello!', { username: 'Piotr', type: 'announcement' });
// { type: 'NOTIFY', from: string, message: string, messageType: MessageType, datetime: Date }
```

[⇧ back to top](#table-of-contents)

---

#### createCustomAction

_Create an enhanced action-creator with unlimited number of arguments and custom properties on action object._
- Arguments of resulting action-creator will preserve their original semantic names `(id, firstName, lastName)`.
- Returned action objects have custom properties `({ type, customProp1, customProp2, ...customPropN })`

```ts
createCustomAction(type, type => {
  return (namedArg1, namedArg2, ...namedArgN) => ({ type, customProp1, customProp2, ...customPropN })
})
```

Examples:
[> Advanced Usage Examples](src/create-action-with-type.spec.ts)

```ts
import { createCustomAction } from 'typesafe-actions';

const add = createCustomAction('CUSTOM', type => {
  return (first: number, second: number) => ({ type, customProp1: first, customProp2: second });
});

add(1) // { type: "CUSTOM"; customProp1: number; customProp2: number; }
```

[⇧ back to top](#table-of-contents)

---

#### createAsyncAction

_Create an object containing three enhanced action-creators to simplify handling of async flows (e.g. network request - request/success/failure)._

```ts
createAsyncAction(requestType, successType, failureType)
```

Examples:
[> Advanced Usage Examples](src/create-async-action.spec.ts)

```ts
import { createAsyncAction } from 'typesafe-actions';

const fetchUsers = createAsyncAction(
  'FETCH_USERS_REQUEST',
  'FETCH_USERS_SUCCESS',
  'FETCH_USERS_FAILURE'
)<string, User[], Error>();

fetchUsers.request(params);

fetchUsers.success(response);

fetchUsers.failure(err);
```

[⇧ back to top](#table-of-contents)

---

### Action-helpers

#### getType

_Get the **type** property value (narrowed to literal type) of given enhanced action-creator._

```ts
getType(actionCreator)
```

[> Advanced Usage Examples](src/get-type.spec.ts)

Examples:
```ts
import { getType, createStandardAction } from 'typesafe-actions';

const add = createStandardAction('ADD')<number>();

// In switch reducer
switch (action.type) {
  case getType(add):
    // action type is { type: "ADD"; payload: number; }
    return state + action.payload;

  default:
    return state;
}

// or with conditional statements
if (action.type === getType(add)) {
  // action type is { type: "ADD"; payload: number; }
}
```

[⇧ back to top](#table-of-contents)

---

#### isActionOf

_Check if action is an instance of given enhanced action-creator(s)
(it will narrow action type to a type of given action-creator(s))_


**NOTE**: ActionCreator type is generated from the `createAction` API. Simple [action](#action) creators throw a `RuntimeError`

```ts
// can be used as a binary function
isActionOf(actionCreator, action)
// or as a curried function
isActionOf(actionCreator)(action)
// also accepts an array
isActionOf([actionCreator1, actionCreator2, ...actionCreatorN], action)
// with its curried equivalent
isActionOf([actionCreator1, actionCreator2, ...actionCreatorN])(action)
```

Examples:
[> Advanced Usage Examples](src/is-action-of.spec.ts)

```ts
import { addTodo, removeTodo } from './todos-actions';

// Works with any filter type function (`Array.prototype.filter`, lodash, ramda, rxjs, etc.)
// - single action
[action1, action2, ...actionN]
  .filter(isActionOf(addTodo)) // only actions with type `ADD` will pass
  .map((action) => {
    // action type is { type: "todos/ADD"; payload: Todo; }
    ...
    
// - multiple actions
[action1, action2, ...actionN]
  .filter(isActionOf([addTodo, removeTodo])) // only actions with type `ADD` or 'REMOVE' will pass
  .do((action) => {
    // action type is { type: "todos/ADD"; payload: Todo; } | { type: "todos/REMOVE"; payload: Todo; }
    ...
      
// With conditional statements
// - single action
if(isActionOf(addTodo, action)) {
  return iAcceptOnlyTodoType(action.payload);
  // action type is { type: "todos/ADD"; payload: Todo; }
}
// - multiple actions
if(isActionOf([addTodo, removeTodo], action)) {
  return iAcceptOnlyTodoType(action.payload);
  // action type is { type: "todos/ADD"; payload: Todo; } | { type: "todos/REMOVE"; payload: Todo; }
}
```

[⇧ back to top](#table-of-contents)

---

#### isOfType

_Check if action type property is equal given type-constant(s)
(it will narrow action type to a type of given action-creator(s))_

```ts
// can be used as a binary function
isOfType(type, action)
// or as curried function
isOfType(type)(action)
// also accepts an array
isOfType([type1, type2, ...typeN], action)
// with its curried equivalent
isOfType([type1, type2, ...typeN])(action)
```

Examples:
[> Advanced Usage Examples](src/is-of-type.spec.ts)

```ts
import { ADD, REMOVE } from './todos-types';

// Works with any filter type function (`Array.prototype.filter`, lodash, ramda, rxjs, etc.)
// - single action
[action1, action2, ...actionN]
  .filter(isOfType(ADD)) // only actions with type `ADD` will pass
  .map((action) => {
    // action type is { type: "todos/ADD"; payload: Todo; }
    ...
    
// - multiple actions
[action1, action2, ...actionN]
  .filter(isOfType([ADD, REMOVE])) // only actions with type `ADD` or 'REMOVE' will pass
  .do((action) => {
    // action type is { type: "todos/ADD"; payload: Todo; } | { type: "todos/REMOVE"; payload: Todo; }
    ...
      
// With conditional statements
// - single action
if(isOfType(ADD, action)) {
  return iAcceptOnlyTodoType(action.payload);
  // action type is { type: "todos/ADD"; payload: Todo; }
}
// - multiple actions
if(isOfType([ADD, REMOVE], action)) {
  return iAcceptOnlyTodoType(action.payload);
  // action type is { type: "todos/ADD"; payload: Todo; } | { type: "todos/REMOVE"; payload: Todo; }
}

```

[⇧ back to top](#table-of-contents)

---

### Action-factory

#### action

_Simple **action factory function** to simplify creation of type-safe actions._

**WARNING**: This approach will **NOT WORK** with **action-helpers** (such as `getType` and `isActionOf`) because it is creating simple actions. All the other creator functions will work because they are creating **enhanced action-creators**.

```ts
action(type, payload?, meta?)
```

Examples:
[> Advanced Usage Examples](src/action.spec.ts)

```ts
const increment = () => action('INCREMENT');
// { type: 'INCREMENT'; }

const createUser = (id: number, name: string) =>
  action('CREATE_USER', { id, name });
// { type: 'CREATE_USER'; payload: { id: number; name: string }; }

const getUsers = (params?: string) =>
  action('GET_USERS', undefined, params);
// { type: 'GET_USERS'; meta: string | undefined; }
```

**NOTICE**: Starting from TypeScript v3.4 you can achieve similar results using new `as const` operator.

```ts
const increment = () => ({ type: 'INCREMENT' } as const);
```

[⇧ back to top](#table-of-contents)

---

## Migration Guides

### v2.x.x to v3.x.x
v3.x.x API is backward compatible with v2.x.x. You'll only need to update typescript dependency to `> v3.1`.

### v1.x.x to v2.x.x
> NOTE: `typesafe-actions@1.x.x` should be used with `utility-types@1.x.x` which contains `$call` utility (removed in `utility-types@2.x.x`)

In v2 we provide `createActionDeprecated` from v1 API to help with incremental migration.

```ts
// in v1 we created action-creator like this:
const getTodo = createAction('GET_TODO',
  (id: string, meta: string) => ({
    type: 'GET_TODO',
    payload: id,
    meta: meta,
  })
);

getTodo('some_id', 'some_meta'); // { type: 'GET_TODO', payload: 'some_id', meta: 'some_meta' }

// in v2 API we offer 4 different styles - please choose your preference
const getTodoSimple = (id: string, meta: string) => action('GET_TODO', id, meta);

const getTodoVariadic = createAction('GET_TODO', resolve => {
  return (id: string, meta: string) => resolve(id, meta);
});

const getTodoStandard = createStandardAction('GET_TODO')<string, string>();

const getTodoStandardWithMap = createStandardAction('GET_TODO').map(
  ({ id, meta }: { id: string; meta: string; }) => ({
    payload: id,
    meta,
  })
);
```

### Migrating from `redux-actions`

If you're using `redux-actions` you can replace `createAction` with any of the above styles. Currently there is no equivalent of `createActions` function, so it will need to be replaced with individual usages of `createAction`. The resulting hash of actions does not provide inference for the individual values.

Additionally, if migrating from JS -> TS, you can swap out `redux-actions` action-creators in your `handleActions` with action-creators from `typesafe-actions`. This works because the action creators exposed by `typesafe-actions` provide the `toString` method used by `redux-actions` to match actions to the correct reducer.

[⇧ back to top](#table-of-contents)

---

## Compare to others

Here you can find out a detailed comparison of `typesafe-actions` to other solutions.

### `redux-actions`
Lets compare the 3 most common variants of action-creators (with type only, with payload and with payload + meta)

Note: tested with "@types/redux-actions": "2.2.3"

#### - with type only (no payload)

##### redux-actions
```ts
const notify1 = createAction('NOTIFY');
// resulting type:
// () => {
//   type: string;
//   payload: void | undefined;
//   error: boolean | undefined;
// }
```
> with `redux-actions` you can notice the redundant nullable `payload` property and literal type of `type` property is lost (discrimination of union type would not be possible)

##### typesafe-actions
```ts
const notify1 = () => action('NOTIFY');
// resulting type:
// () => {
//   type: "NOTIFY";
// }
```
> with `typesafe-actions` there is no excess nullable types and no excess properties and the action "type" property is containing a literal type

#### - with payload

##### redux-actions
```ts
const notify2 = createAction('NOTIFY',
  (username: string, message?: string) => ({
    message: `${username}: ${message || 'Empty!'}`,
  })
);
// resulting type:
// (t1: string) => {
//   type: string;
//   payload: { message: string; } | undefined;
//   error: boolean | undefined;
// }
```
> first the optional `message` parameter is lost, `username` semantic argument name is changed to some generic `t1`, `type` property is widened once again and `payload` is nullable because of broken inference

##### typesafe-actions
```ts
const notify2 = (username: string, message?: string) => action(
  'NOTIFY',
  { message: `${username}: ${message || 'Empty!'}` },
);
// resulting type:
// (username: string, message?: string | undefined) => {
//   type: "NOTIFY";
//   payload: { message: string; };
// }
```
> `typesafe-actions` infer very precise resulting type, notice working optional parameters and semantic argument names are preserved which is really important for great intellisense experience

#### - with payload and meta

##### redux-actions
```ts
const notify3 = createAction('NOTIFY',
  (username: string, message?: string) => (
    { message: `${username}: ${message || 'Empty!'}` }
  ),
  (username: string, message?: string) => (
    { username, message }
  )
);
// resulting type:
// (...args: any[]) => {
//   type: string;
//   payload: { message: string; } | undefined;
//   meta: { username: string; message: string | undefined; };
//   error: boolean | undefined;
// }
```
> this time we got a completely broken arguments arity with no type-safety because of `any` type with all the earlier issues

##### typesafe-actions
```ts
/**
 * typesafe-actions
 */
const notify3 = (username: string, message?: string) => action(
  'NOTIFY',
  { message: `${username}: ${message || 'Empty!'}` },
  { username, message },
);
// resulting type:
// (username: string, message?: string | undefined) => {
//   type: "NOTIFY";
//   payload: { message: string; };
//   meta: { username: string; message: string | undefined; };
// }
```
> `typesafe-actions` never fail to `any` type, even with this advanced scenario all types are correct and provide complete type-safety and excellent developer experience 

[⇧ back to top](#table-of-contents)

---

MIT License

Copyright (c) 2017 Piotr Witek <piotrek.witek@gmail.com> (http://piotrwitek.github.io)
