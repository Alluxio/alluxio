import { TypeMeta } from './types';

export type ActionCreator<T extends { type: string }> = ((
  ...args: any[]
) => T) &
  TypeMeta<T['type']>;

/**
 * @description (curried assert function) check if an action is the instance of given action-creator(s)
 * @description it works with discriminated union types
 */
export function isActionOf<AC extends ActionCreator<{ type: string }>>(
  actionCreator: AC | AC[],
  action: { type: string }
): action is ReturnType<AC>;

/**
 * @description (curried assert function) check if an action is the instance of given action-creator(s)
 * @description it works with discriminated union types
 */
export function isActionOf<AC extends ActionCreator<{ type: string }>>(
  actionCreator: AC | AC[]
): (action: { type: string }) => action is ReturnType<AC>;

/** implementation */
export function isActionOf<AC extends ActionCreator<{ type: string }>>(
  creatorOrCreators: AC | AC[],
  actionOrNil?: { type: string }
) {
  if (creatorOrCreators == null) {
    throw new Error('first argument is missing');
  }

  if (Array.isArray(creatorOrCreators)) {
    (creatorOrCreators as any[]).forEach((actionCreator, index) => {
      if (actionCreator.getType == null) {
        throw new Error(`first argument contains element
        that is not created with "typesafe-actions" at index [${index}]`);
      }
    });
  } else {
    if (creatorOrCreators.getType == null) {
      throw new Error('first argument is not created with "typesafe-actions"');
    }
  }

  const assertFn = (action: { type: string }) => {
    const actionCreators: any[] = Array.isArray(creatorOrCreators)
      ? creatorOrCreators
      : [creatorOrCreators];

    return actionCreators.some((actionCreator, index) => {
      return actionCreator.getType() === action.type;
    });
  };

  // with 1 arg return assertFn
  if (actionOrNil == null) {
    return assertFn;
  }
  // with 2 args invoke assertFn and return the result
  return assertFn(actionOrNil);
}
