/**
 * @author Piotr Witek <piotrek.witek@gmail.com> (http://piotrwitek.github.io)
 * @copyright Copyright (c) 2017 Piotr Witek
 * @license MIT
 */

/** Public API */
// creators
export { action } from './action';
export { createAction } from './create-action';
export { createStandardAction } from './create-standard-action';
export { createCustomAction } from './create-custom-action';
export { createAsyncAction } from './create-async-action';
// guards
export { ActionType, StateType } from './types';
export { getType } from './get-type';
export { isOfType } from './is-of-type';
export { isActionOf } from './is-action-of';
// deprecated
export { createActionDeprecated } from './create-action-deprecated';
