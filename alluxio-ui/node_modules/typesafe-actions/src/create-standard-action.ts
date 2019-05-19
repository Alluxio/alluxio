import { StringType, Box, FsaBuilder, FsaMapBuilder } from './types';
import { createCustomAction } from './create-custom-action';
import { validateActionType } from './utils/utils';

export interface CreateStandardAction<T extends StringType> {
  <P = void, M = void>(): FsaBuilder<T, Box<P>, Box<M>>;
  map<R, P = void, M = void>(
    fn: (payload: P, meta: M) => R
  ): FsaMapBuilder<T, Box<R>, Box<P>, Box<M>>;
}

/**
 * @description create an action-creator of a given function that contains hidden "type" metadata
 */
export function createStandardAction<T extends StringType>(
  actionType: T
): CreateStandardAction<T> {
  validateActionType(actionType);

  function constructor<P, M = void>(): FsaBuilder<T, Box<P>, Box<M>> {
    return createCustomAction(actionType, type => (payload: P, meta: M) => ({
      type,
      payload,
      meta,
    })) as FsaBuilder<T, Box<P>, Box<M>>;
  }

  function map<R, P, M>(
    fn: (payload: P, meta: M) => R
  ): FsaMapBuilder<T, Box<R>, Box<P>, Box<M>> {
    return createCustomAction(actionType, type => (payload: P, meta: M) =>
      Object.assign(fn(payload, meta), { type })
    ) as FsaMapBuilder<T, Box<R>, Box<P>, Box<M>>;
  }

  return Object.assign(constructor, { map });
}
