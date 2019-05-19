import {
  StringType,
  EmptyAction,
  PayloadAction,
  PayloadMetaAction,
} from './types';

export function action<T extends StringType>(type: T): EmptyAction<T>;

export function action<T extends StringType, P = undefined>(
  type: T,
  payload: P
): PayloadAction<T, P>;

export function action<T extends StringType, P = undefined, M = undefined>(
  type: T,
  payload: P,
  meta: M
): PayloadMetaAction<T, P, M>;

/**
 * @description flux standard action factory
 * @example
 * ```
 * const add = (amount: number, meta?: MetaShape) => action('INCREMENT', amount, meta);
 * ```
 */
export function action<T extends StringType, P = undefined, M = undefined>(
  type: T,
  payload?: P,
  meta?: M
) {
  return { type, payload, meta } as any;
}
