/**
 * PUBLIC API
 */

/**
 * @desc Infers Action union-type from action-creator map object
 */
export type ActionType<
  ActionCreatorOrMap
> = ActionCreatorOrMap extends ActionCreator<StringOrSymbol>
  ? ReturnType<ActionCreatorOrMap>
  : ActionCreatorOrMap extends object
  ? ActionCreatorMap<ActionCreatorOrMap>[keyof ActionCreatorOrMap]
  : never;

/**
 * @desc Infers State object from reducer map object
 */
export type StateType<ReducerOrMap> = ReducerOrMap extends (
  ...args: any[]
) => any
  ? ReturnType<ReducerOrMap>
  : ReducerOrMap extends object
  ? { [K in keyof ReducerOrMap]: StateType<ReducerOrMap[K]> }
  : never;

/**
 * INTERNAL API
 */

/**
 * @private
 * @desc Representing action-type of string
 */
export type StringType = string;

/**
 * @private
 * @desc Representing action-type of symbol
 */
export type SymbolType = symbol;

/**
 * @private
 * @desc Representing generic action-type
 */
export type StringOrSymbol = StringType | SymbolType;

/**
 * @private
 * @desc Action without Payload
 * @type T - ActionType
 */
export type EmptyAction<T extends StringType> = {
  type: T;
};

/**
 * @private
 * @desc Action with only Payload
 * @type T - ActionType
 * @type P - Payload
 */
export type PayloadAction<T extends StringType, P> = {
  type: T;
  payload: P;
};

/**
 * @private
 * @desc Action with only Meta
 * @type T - ActionType
 * @type P - Payload
 * @type M - Meta
 */
export type MetaAction<T extends StringType, M> = {
  type: T;
  meta: M;
};

/**
 * @private
 * @desc Action with both Payload and Meta
 * @type T - ActionType
 * @type P - Payload
 * @type M - Meta
 */
export type PayloadMetaAction<T extends StringType, P, M> = {
  type: T;
  payload: P;
  meta: M;
};

/**
 * TODO: NOT USED
 * @private
 * @desc Flux Standard Action
 * @type T - ActionType
 * @type P - Payload
 * @type M - Meta
 */
export interface FluxStandardAction<T extends StringType, P = void, M = void> {
  type: T;
  payload: P;
  meta: M;
  error?: true;
}

/** @private */
export interface TypeMeta<T extends StringType> {
  getType?: () => T;
}

/** @private */
export type Box<T> = { v: T };

/** @private */
export type Unbox<T extends Box<any>> = T['v'];

/** @private */
export type NoArgCreator<T extends StringType> = () => EmptyAction<T>;

/** @private */
export type PayloadCreator<T extends StringType, P> = (
  payload: P
) => PayloadAction<T, P>;

/** @private */
export type PayloadMetaCreator<T extends StringType, P, M> = (
  payload: P,
  meta: M
) => PayloadMetaAction<T, P, M>;

/** @private */
export type FsaBuilder<
  T extends StringType,
  P extends Box<any> = Box<void>,
  M extends Box<any> = Box<void>
> = M extends Box<void>
  ? P extends Box<void>
    ? NoArgCreator<T>
    : PayloadCreator<T, Unbox<P>>
  : PayloadMetaCreator<T, Unbox<P>, Unbox<M>>;

/** @private */
export type FsaMapBuilder<
  T extends StringType,
  R extends Box<any>,
  P extends Box<any> = Box<void>,
  M extends Box<any> = Box<void>
> = M extends Box<void>
  ? P extends Box<void>
    ? () => { type: T } & Unbox<R>
    : (payload: Unbox<P>) => { type: T } & Unbox<R>
  : (payload: Unbox<P>, meta: Unbox<M>) => { type: T } & Unbox<R>;

/** @private */
export type ActionCreator<T extends StringOrSymbol> = (
  ...args: any[]
) => { type: T };

/** @private */
export type ActionCreatorMap<T> = { [K in keyof T]: ActionType<T[K]> };
