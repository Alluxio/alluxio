import * as Types from './types';
import { createStandardAction } from './create-standard-action';

describe('createStandardAction', () => {
  describe('constructor', () => {
    it('toString', () => {
      const increment = createStandardAction('INCREMENT')();

      expect(increment.toString()).toBe('INCREMENT');
      // tslint:disable-next-line:triple-equals
      expect((increment as any) == 'INCREMENT').toBe(true);
    });

    it('with type only - shorthand', () => {
      const increment = createStandardAction('INCREMENT')();
      const actual = increment();
      // @dts-jest:pass:snap -> Types.EmptyAction<"INCREMENT">
      actual;
      expect(actual).toEqual({ type: 'INCREMENT' });
    });

    it('with type only', () => {
      const increment = createStandardAction('INCREMENT')<void>();
      const action: { type: 'INCREMENT' } = increment();
      expect(action).toEqual({ type: 'INCREMENT' });
    });

    it('with payload - number', () => {
      const add = createStandardAction('WITH_MAPPED_PAYLOAD')<number>();
      const action: { type: 'WITH_MAPPED_PAYLOAD'; payload: number } = add(10);
      expect(action).toEqual({ type: 'WITH_MAPPED_PAYLOAD', payload: 10 });
    });

    it('with payload - boolean', () => {
      const set = createStandardAction('SET')<boolean>();
      const action: { type: 'SET'; payload: boolean } = set(true);
      expect(action).toEqual({ type: 'SET', payload: true });
    });

    it('with payload - literal string union', () => {
      type NetStatus = 'up' | 'down' | 'unknown';
      const set = createStandardAction('SET')<NetStatus>();
      const action: { type: 'SET'; payload: NetStatus } = set('up');
      expect(action).toEqual({ type: 'SET', payload: 'up' });
    });

    it('with payload - primitives union', () => {
      const union = createStandardAction('UNION')<string | null | number>();
      const action: { type: 'UNION'; payload: string | null | number } = union(
        'foo'
      );
      expect(action).toEqual({ type: 'UNION', payload: 'foo' });
      const action2: { type: 'UNION'; payload: string | null | number } = union(
        null
      );
      expect(action2).toEqual({ type: 'UNION', payload: null });
      const action3: { type: 'UNION'; payload: string | null | number } = union(
        3
      );
      expect(action3).toEqual({ type: 'UNION', payload: 3 });
    });

    it('with meta', () => {
      const action = createStandardAction('WITH_META')<void, string>();
      const actual: { type: 'WITH_META'; meta: string } = action(
        undefined,
        'token'
      );
      expect(actual).toEqual({ type: 'WITH_META', meta: 'token' });
    });

    it('with payload and meta', () => {
      const action = createStandardAction('WITH_PAYLOAD_META')<
        number,
        string
      >();
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
  });

  describe('map', () => {
    it('with type only', () => {
      const increment = createStandardAction('INCREMENT').map(() => ({}));
      const action: { type: 'INCREMENT' } = increment();
      expect(action).toEqual({ type: 'INCREMENT' });
    });

    it('with payload - no param', () => {
      const showNotification = createStandardAction('SHOW_NOTIFICATION').map(
        () => ({
          payload: 'hardcoded message',
        })
      );
      const action: {
        type: 'SHOW_NOTIFICATION';
        payload: string;
      } = showNotification();
      expect(action).toEqual({
        type: 'SHOW_NOTIFICATION',
        payload: 'hardcoded message',
      });
    });

    it('with payload - primitive param', () => {
      const showNotification = createStandardAction('SHOW_NOTIFICATION').map(
        (payload: string) => ({
          payload,
        })
      );
      const action: {
        type: 'SHOW_NOTIFICATION';
        payload: string;
      } = showNotification('info message');
      expect(action).toEqual({
        type: 'SHOW_NOTIFICATION',
        payload: 'info message',
      });
    });

    it('with payload - union param', () => {
      const showNotification = createStandardAction('SHOW_NOTIFICATION').map(
        (payload: string | null | number) => ({
          payload,
        })
      );
      const action: {
        type: 'SHOW_NOTIFICATION';
        payload: string | null | number;
      } = showNotification('info message');
      expect(action).toEqual({
        type: 'SHOW_NOTIFICATION',
        payload: 'info message',
      });
      const action2: {
        type: 'SHOW_NOTIFICATION';
        payload: string | null | number;
      } = showNotification(null);
      expect(action2).toEqual({
        type: 'SHOW_NOTIFICATION',
        payload: null,
      });
      const action3: {
        type: 'SHOW_NOTIFICATION';
        payload: string | null | number;
      } = showNotification(3);
      expect(action3).toEqual({
        type: 'SHOW_NOTIFICATION',
        payload: 3,
      });
    });

    it('with meta - no param', () => {
      const showNotification = createStandardAction('SHOW_NOTIFICATION').map(
        () => ({
          meta: 'hardcoded message',
        })
      );
      const action: {
        type: 'SHOW_NOTIFICATION';
        meta: string;
      } = showNotification();
      expect(action).toEqual({
        type: 'SHOW_NOTIFICATION',
        meta: 'hardcoded message',
      });
    });

    it('with meta - primitive param', () => {
      const showNotification = createStandardAction('SHOW_NOTIFICATION').map(
        (meta: string) => ({
          meta,
        })
      );
      const action: {
        type: 'SHOW_NOTIFICATION';
        meta: string;
      } = showNotification('info message');
      expect(action).toEqual({
        type: 'SHOW_NOTIFICATION',
        meta: 'info message',
      });
    });

    it('with meta - union param', () => {
      const showNotification = createStandardAction('SHOW_NOTIFICATION').map(
        (meta: string | null | number) => ({
          meta,
        })
      );
      const action: {
        type: 'SHOW_NOTIFICATION';
        meta: string | null | number;
      } = showNotification('info message');
      expect(action).toEqual({
        type: 'SHOW_NOTIFICATION',
        meta: 'info message',
      });
      const action2: {
        type: 'SHOW_NOTIFICATION';
        meta: string | null | number;
      } = showNotification(null);
      expect(action2).toEqual({ type: 'SHOW_NOTIFICATION', meta: null });
      const action3: {
        type: 'SHOW_NOTIFICATION';
        meta: string | null | number;
      } = showNotification(3);
      expect(action3).toEqual({ type: 'SHOW_NOTIFICATION', meta: 3 });
    });

    it('with payload and meta - no param', () => {
      const showError = createStandardAction('SHOW_ERROR').map(() => ({
        payload: 'hardcoded error',
        meta: { severity: 'error' },
      }));
      const action: {
        type: 'SHOW_ERROR';
        payload: string;
        meta: { severity: string };
      } = showError();
      expect(action).toEqual({
        type: 'SHOW_ERROR',
        payload: 'hardcoded error',
        meta: { severity: 'error' },
      });
    });

    it('with payload and meta - string param', () => {
      const showError = createStandardAction('SHOW_ERROR').map(
        (message: string) => ({
          payload: message,
          meta: { severity: 'error' },
        })
      );
      const action: {
        type: 'SHOW_ERROR';
        payload: string;
        meta: { severity: string };
      } = showError('error message');
      expect(action).toEqual({
        type: 'SHOW_ERROR',
        payload: 'error message',
        meta: { severity: 'error' },
      });
    });

    it('with payload and meta - object param', () => {
      type Notification = { username: string; message?: string };
      const notify = createStandardAction('WITH_PAYLOAD_META').map(
        ({ username, message }: Notification) => ({
          payload: `${username}: ${message || ''}`,
          meta: { username, message },
        })
      );
      const action: {
        type: 'WITH_PAYLOAD_META';
        payload: string;
        meta: Notification;
      } = notify({ username: 'Piotr', message: 'Hello!' });
      expect(action).toEqual({
        type: 'WITH_PAYLOAD_META',
        payload: 'Piotr: Hello!',
        meta: { username: 'Piotr', message: 'Hello!' },
      });
    });
  });
});
