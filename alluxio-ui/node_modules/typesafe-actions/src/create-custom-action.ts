import { ActionCreator, StringOrSymbol } from './types';

/**
 * @description create custom action-creator using constructor function with injected type argument
 */
export function createCustomAction<
  T extends StringOrSymbol,
  AC extends ActionCreator<T> = () => { type: T }
>(type: T, actionCreatorHandler?: (type: T) => AC): AC {
  const actionCreator: AC =
    actionCreatorHandler != null
      ? actionCreatorHandler(type)
      : ((() => ({ type })) as AC);

  return Object.assign(actionCreator, {
    getType: () => type,
    // redux-actions compatibility
    toString: () => type,
  });
}
