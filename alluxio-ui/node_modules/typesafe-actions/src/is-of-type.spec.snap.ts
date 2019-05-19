import * as Types from './types';
import { isOfType } from './is-of-type';
import { types, actions } from './utils/test-utils';
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

describe('isOfType', () => {
  it('should work with single action-type arg', () => {
    expect(isOfType(types.WITH_TYPE_ONLY)(typeOnlyAction)).toBeTruthy();
    expect(isOfType(types.WITH_TYPE_ONLY, typeOnlyAction)).toBeTruthy();
    expect(isOfType(types.WITH_TYPE_ONLY)(payloadAction)).toBeFalsy();
    expect(isOfType(types.WITH_TYPE_ONLY, payloadAction)).toBeFalsy();
    expect(isOfType([types.WITH_TYPE_ONLY])(typeOnlyAction)).toBeTruthy();
    expect(isOfType([types.WITH_TYPE_ONLY], typeOnlyAction)).toBeTruthy();
    expect(isOfType([types.WITH_TYPE_ONLY])(payloadAction)).toBeFalsy();
    expect(isOfType([types.WITH_TYPE_ONLY], payloadAction)).toBeFalsy();
  });

  it('should work with multiple action-type args', () => {
    expect(
      isOfType([types.WITH_TYPE_ONLY, types.WITH_PAYLOAD])(typeOnlyAction)
    ).toBeTruthy();
    expect(
      isOfType([types.WITH_TYPE_ONLY, types.WITH_PAYLOAD], typeOnlyAction)
    ).toBeTruthy();
    expect(
      isOfType([types.WITH_TYPE_ONLY, types.WITH_PAYLOAD])(payloadAction)
    ).toBeTruthy();
    expect(
      isOfType([types.WITH_TYPE_ONLY, types.WITH_PAYLOAD], payloadAction)
    ).toBeTruthy();
    expect(
      isOfType([types.WITH_TYPE_ONLY, types.WITH_PAYLOAD])(mappedPayloadAction)
    ).toBeFalsy();
    expect(
      isOfType([types.WITH_TYPE_ONLY, types.WITH_PAYLOAD], mappedPayloadAction)
    ).toBeFalsy();
  });

  it('should correctly assert for an array with 1 arg', () => {
    const actual = $action.filter(isOfType(types.WITH_TYPE_ONLY));
    // @dts-jest:pass:snap -> Types.EmptyAction<"WITH_TYPE_ONLY">[]
    actual;
    expect(actual).toHaveLength(1);
    expect(actual).toEqual([typeOnlyExpected]);
  });

  it('should correctly assert for an array with 2 args', () => {
    const actual = $action.filter(
      isOfType([types.WITH_TYPE_ONLY, types.WITH_PAYLOAD])
    );
    // @dts-jest:pass:snap -> (Types.EmptyAction<"WITH_TYPE_ONLY"> | Types.PayloadAction<"WITH_PAYLOAD", number>)[]
    actual;
    expect(actual).toHaveLength(2);
    expect(actual).toEqual([typeOnlyExpected, payloadExpected]);
  });

  it('should correctly assert for an array with 3 args', () => {
    const actual = $action.filter(
      isOfType([
        types.WITH_TYPE_ONLY,
        types.WITH_PAYLOAD,
        types.WITH_PAYLOAD_META,
      ])
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
      isOfType([
        types.WITH_TYPE_ONLY,
        types.WITH_PAYLOAD,
        types.WITH_PAYLOAD_META,
        types.WITH_MAPPED_PAYLOAD,
      ])
    );
    // @dts-jest:pass:snap -> (Types.EmptyAction<"WITH_TYPE_ONLY"> | Types.PayloadAction<"WITH_PAYLOAD", number> | Types.PayloadMetaAction<"WITH_PAYLOAD_META", number, string> | ({ type: "WITH_MAPPED_PAYLOAD"; } & { payload: number; }))[]
    actual;
    expect(actual).toHaveLength(4);
    expect(actual).toEqual([
      typeOnlyExpected,
      payloadExpected,
      payloadMetaExpected,
      mappedPayloadExpected,
    ]);
  });

  it('should correctly assert for an array with 4 args', () => {
    const actual = $action.filter(
      isOfType([
        types.WITH_TYPE_ONLY,
        types.WITH_PAYLOAD,
        types.WITH_PAYLOAD_META,
        types.WITH_MAPPED_PAYLOAD,
        types.WITH_MAPPED_PAYLOAD_META,
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
    if (
      isOfType(
        [types.WITH_MAPPED_PAYLOAD, types.WITH_MAPPED_PAYLOAD_META],
        action
      )
    ) {
      // @dts-jest:pass:snap -> any
      action;
      expect(action.payload).toBe(1234);
    }
    if (
      isOfType([types.WITH_MAPPED_PAYLOAD, types.WITH_MAPPED_PAYLOAD_META])(
        action
      )
    ) {
      // @dts-jest:pass:snap -> any
      action;
      expect(action.payload).toBe(1234);
    }
  });
});
