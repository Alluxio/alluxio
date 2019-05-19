

// Babel 5.x doesn't support type parameters, so we make this alias here out of
// Babel's sight.
/* eslint-disable spaced-comment, no-undef */
/*::
import type {Element} from 'react';
export type ReactElement = Element<*>;
*/

// === basic reused types ===
// type of the second parameter of `spring(val, config)` all fields are optional
"use strict";

// the object returned by `spring(value, yourConfig)`. For internal usage only!

// your typical style object given in props. Maps to a number or a spring config

// the interpolating style object, with the same keys as the above Style object,
// with the values mapped to numbers, naturally

// internal velocity object. Similar to PlainStyle, but whose numbers represent
// speed. Might be exposed one day.

// === Motion ===

// === StaggeredMotion ===

// === TransitionMotion ===
// actual style you're passing
exports.__esModule = true;
// unique ID to identify component across render animations
// optional data you want to carry along the style, e.g. itemText

// same as TransitionStyle, passed as argument to style/children function