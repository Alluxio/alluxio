import * as Types from './types';
import { createActionDeprecated } from './create-action-deprecated';

describe('createActionDeprecated', () => {
  it('should work with symbol as action type', () => {
    const INCREMENT = Symbol(1);
    const increment = createActionDeprecated(INCREMENT);
    const actual = increment();
    // @dts-jest:pass:snap -> { type: unique symbol; }
    actual;
    expect(actual).toEqual({ type: INCREMENT });
    expect(actual).not.toEqual({ type: 'INCREMENT' });
  });

  it('with type only', () => {
    const increment = createActionDeprecated('INCREMENT');

    const action: { type: 'INCREMENT' } = increment();
    expect(action).toEqual({ type: 'INCREMENT' });
  });

  it('with payload', () => {
    const add = createActionDeprecated('ADD', (amount: number) => ({
      type: 'ADD',
      payload: amount,
    }));

    const action: { type: 'ADD'; payload: number } = add(10);
    expect(action).toEqual({ type: 'ADD', payload: 10 });
  });

  it('with optional payload', () => {
    const showNotification = createActionDeprecated(
      'SHOW_NOTIFICATION',
      (message?: string) => ({
        type: 'SHOW_NOTIFICATION',
        payload: message,
      })
    );

    const action: {
      type: 'SHOW_NOTIFICATION';
      payload: string | undefined;
    } = showNotification();
    expect(action).toEqual({
      type: 'SHOW_NOTIFICATION',
      payload: undefined,
    });
  });

  it('with meta', () => {
    const showError = createActionDeprecated(
      'SHOW_ERROR',
      (message: string) => ({
        type: 'SHOW_ERROR',
        meta: message,
      })
    );

    const action: { type: 'SHOW_ERROR'; meta: string } = showError(
      'Error message'
    );
    expect(action).toEqual({
      type: 'SHOW_ERROR',
      meta: 'Error message',
    });
  });

  it('with optional meta', () => {
    const showError = createActionDeprecated(
      'SHOW_ERROR',
      (message?: string) => ({
        type: 'SHOW_ERROR',
        meta: message,
      })
    );

    const action: { type: 'SHOW_ERROR'; meta: string | undefined } = showError(
      'Error message'
    );
    expect(action).toEqual({
      type: 'SHOW_ERROR',
      meta: 'Error message',
    });
  });

  it('with payload and meta', () => {
    const notify = createActionDeprecated(
      'NOTIFY',
      (username: string, message: string) => ({
        type: 'NOTIFY',
        payload: { message: `${username}: ${message}` },
        meta: { username, message },
      })
    );

    const action: {
      type: 'NOTIFY';
      payload: { message: string };
      meta: { username: string; message: string };
    } = notify('Piotr', 'Hello!');
    expect(action).toEqual({
      type: 'NOTIFY',
      payload: { message: 'Piotr: Hello!' },
      meta: { username: 'Piotr', message: 'Hello!' },
    });
  });
});
