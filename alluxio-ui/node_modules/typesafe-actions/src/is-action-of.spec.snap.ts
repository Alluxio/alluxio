import * as Types from './types';
import { isActionOf } from './is-action-of';
import { actions } from './utils/test-utils';
const {
  withTypeOnly,
  withPayload,
  withPayloadMeta,
  withMappedPayload,
  withMappedPayloadMeta,
} = actions;

/** FIXTURES */
const typeOnlyAction = withTypeOnly();
const typeOnlyExpected = { type: 'WITH_TYPE_ONLY' };
const payloadAction = withPayload(2);
const payloadExpected = { type: 'WITH_PAYLOAD', payload: 2 };
const payloadMetaAction = withPayloadMeta(2, 'metaValue');
const payloadMetaExpected = {
  type: 'WITH_PAYLOAD_META',
  payload: 2,
  meta: 'metaValue',
};
const mappedPayloadAction = withMappedPayload(2);
const mappedPayloadExpected = { type: 'WITH_MAPPED_PAYLOAD', payload: 2 };
const mappedPayloadMetaAction = withMappedPayloadMeta(2, 'metaValue');
const mappedPayloadMetaExpected = {
  type: 'WITH_MAPPED_PAYLOAD_META',
  payload: 2,
  meta: 'metaValue',
};

const $action = [
  typeOnlyAction,
  payloadAction,
  payloadMetaAction,
  mappedPayloadAction,
  mappedPayloadMetaAction,
];

describe('isActionOf', () => {
  it('should work with single action-creator arg', () => {
    expect(isActionOf(withTypeOnly)(typeOnlyAction)).toBeTruthy();
    expect(isActionOf(withTypeOnly, typeOnlyAction)).toBeTruthy();
    expect(isActionOf(withTypeOnly)(payloadAction)).toBeFalsy();
    expect(isActionOf(withTypeOnly, payloadAction)).toBeFalsy();
    expect(isActionOf([withTypeOnly])(typeOnlyAction)).toBeTruthy();
    expect(isActionOf([withTypeOnly], typeOnlyAction)).toBeTruthy();
    expect(isActionOf([withTypeOnly])(payloadAction)).toBeFalsy();
    expect(isActionOf([withTypeOnly], payloadAction)).toBeFalsy();
  });

  it('should work with multiple action-creator args', () => {
    expect(
      isActionOf([withTypeOnly, withPayload])(typeOnlyAction)
    ).toBeTruthy();
    expect(
      isActionOf([withTypeOnly, withPayload], typeOnlyAction)
    ).toBeTruthy();
    expect(isActionOf([withTypeOnly, withPayload])(payloadAction)).toBeTruthy();
    expect(isActionOf([withTypeOnly, withPayload], payloadAction)).toBeTruthy();
    expect(
      isActionOf([withTypeOnly, withPayload])(mappedPayloadAction)
    ).toBeFalsy();
    expect(
      isActionOf([withTypeOnly, withPayload], mappedPayloadAction)
    ).toBeFalsy();
  });

  it('should correctly assert for an array with 1 arg', () => {
    const actual = $action.filter(isActionOf([withTypeOnly]));
    // @dts-jest:pass:snap -> Types.EmptyAction<"WITH_TYPE_ONLY">[]
    actual;
    expect(actual).toHaveLength(1);
    expect(actual).toEqual([typeOnlyExpected]);
  });

  it('should correctly assert for an array with 2 args', () => {
    const actual = $action.filter(isActionOf([withTypeOnly, withPayload]));
    // @dts-jest:pass:snap -> (Types.EmptyAction<"WITH_TYPE_ONLY"> | Types.PayloadAction<"WITH_PAYLOAD", number>)[]
    actual;
    expect(actual).toHaveLength(2);
    expect(actual).toEqual([typeOnlyExpected, payloadExpected]);
  });

  it('should correctly assert for an array with 3 args', () => {
    const actual = $action.filter(
      isActionOf([withTypeOnly, withPayload, withPayloadMeta])
    );
    // @dts-jest:pass:snap -> (Types.EmptyAction<"WITH_TYPE_ONLY"> | Types.PayloadAction<"WITH_PAYLOAD", number> | Types.PayloadMetaAction<"WITH_PAYLOAD_META", number, string>)[]
    actual;
    expect(actual).toHaveLength(3);
    expect(actual).toEqual([
      typeOnlyExpected,
      payloadExpected,
      payloadMetaExpected,
    ]);
  });

  it('should correctly assert for an array with 4 args', () => {
    const actual = $action.filter(
      isActionOf([
        withTypeOnly,
        withPayload,
        withPayloadMeta,
        withMappedPayload,
      ])
    );
    // @dts-jest:pass:snap -> (Types.EmptyAction<"WITH_TYPE_ONLY"> | Types.PayloadAction<"WITH_PAYLOAD", number> | Types.PayloadMetaAction<"WITH_PAYLOAD_META", number, string> | ({ type: "WITH_MAPPED_PAYLOAD"; } & { payload: number; }))[]
    actual;
    expect(actual).toEqual([
      typeOnlyExpected,
      payloadExpected,
      payloadMetaExpected,
      mappedPayloadExpected,
    ]);
  });

  it('should correctly assert for an array with 5 args', () => {
    const actual = $action.filter(
      isActionOf([
        withTypeOnly,
        withPayload,
        withPayloadMeta,
        withMappedPayload,
        withMappedPayloadMeta,
      ])
    );
    // @dts-jest:pass:snap -> (Types.EmptyAction<"WITH_TYPE_ONLY"> | Types.PayloadAction<"WITH_PAYLOAD", number> | Types.PayloadMetaAction<"WITH_PAYLOAD_META", number, string> | ({ type: "WITH_MAPPED_PAYLOAD"; } & { payload: number; }) | ({ type: "WITH_MAPPED_PAYLOAD_META"; } & { payload: number; meta: string; }))[]
    actual;
    expect(actual).toHaveLength(5);
    expect(actual).toEqual([
      typeOnlyExpected,
      payloadExpected,
      payloadMetaExpected,
      mappedPayloadExpected,
      mappedPayloadMetaExpected,
    ]);
  });

  it('should correctly assert type with "any" action', () => {
    const action: any = withMappedPayload(1234);
    if (isActionOf([withMappedPayload, withMappedPayloadMeta], action)) {
      // @dts-jest:pass:snap -> ({ type: "WITH_MAPPED_PAYLOAD"; } & { payload: number; }) | ({ type: "WITH_MAPPED_PAYLOAD_META"; } & { payload: number; meta: string; })
      action;
      expect(action.payload).toBe(1234);
    }
    if (isActionOf([withMappedPayload, withMappedPayloadMeta])(action)) {
      // @dts-jest:pass:snap -> ({ type: "WITH_MAPPED_PAYLOAD"; } & { payload: number; }) | ({ type: "WITH_MAPPED_PAYLOAD_META"; } & { payload: number; meta: string; })
      action;
      expect(action.payload).toBe(1234);
    }
  });
});
