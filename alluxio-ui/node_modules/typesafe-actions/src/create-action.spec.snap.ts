import * as Types from './types';
import { createAction } from './create-action';
import { types } from './utils/test-utils';

describe('createAction', () => {
  it('toString', () => {
    const action = createAction(types.WITH_TYPE_ONLY, resolve => {
      return () => resolve();
    });
    expect(action.toString()).toBe('WITH_TYPE_ONLY');
    // tslint:disable-next-line:triple-equals
    expect((action as any) == 'WITH_TYPE_ONLY').toBe(true);
  });

  it('with type only - shorthand', () => {
    const action = createAction(types.WITH_TYPE_ONLY);
    const actual = action();
    // @dts-jest:pass:snap -> { type: "WITH_TYPE_ONLY"; }
    actual;
    expect(actual).toEqual({ type: 'WITH_TYPE_ONLY' });
  });

  it('with type only', () => {
    const action = createAction(types.WITH_TYPE_ONLY, resolve => {
      return () => resolve();
    });
    const actual: { type: 'WITH_TYPE_ONLY' } = action();
    expect(actual).toEqual({ type: 'WITH_TYPE_ONLY' });
  });

  it('with payload', () => {
    const action = createAction(types.WITH_PAYLOAD, resolve => {
      return (id: number) => resolve(id);
    });
    const actual: { type: 'WITH_PAYLOAD'; payload: number } = action(1);
    expect(actual).toEqual({ type: 'WITH_PAYLOAD', payload: 1 });
  });

  it('with meta', () => {
    const action = createAction(types.WITH_META, resolve => {
      return (token: string) => resolve(undefined, token);
    });
    const actual: { type: 'WITH_META'; meta: string } = action('token');
    expect(actual).toEqual({ type: 'WITH_META', meta: 'token' });
  });

  it('with payload and meta', () => {
    const action = createAction(types.WITH_PAYLOAD_META, resolve => {
      return (id: number, token: string) => resolve(id, token);
    });
    const actual: {
      type: 'WITH_PAYLOAD_META';
      payload: number;
      meta: string;
    } = action(1, 'token');
    expect(actual).toEqual({
      type: 'WITH_PAYLOAD_META',
      payload: 1,
      meta: 'token',
    });
  });

  it('with higher-order function', () => {
    interface UserSettingsState {
      settingA: string;
      settingB: number;
    }

    const setUserSetting = <K extends keyof UserSettingsState>(
      setting: K,
      newValue: UserSettingsState[K]
    ) =>
      createAction('SET_USER_SETTING', resolve => () =>
        resolve({ setting, newValue })
      )();

    // @dts-jest:pass:snap -> { type: "SET_USER_SETTING"; payload: { setting: "settingA"; newValue: string; }; }
    setUserSetting('settingA', 'foo');
    // @dts-jest:fail:snap -> Argument of type '0' is not assignable to parameter of type 'string'.
    setUserSetting('settingA', 0); // Error as expected

    // @dts-jest:pass:snap -> { type: "SET_USER_SETTING"; payload: { setting: "settingB"; newValue: number; }; }
    setUserSetting('settingB', 0);
    // @dts-jest:fail:snap -> Argument of type '"foo"' is not assignable to parameter of type 'number'.
    setUserSetting('settingB', 'foo'); // Error as expected
  });
});
