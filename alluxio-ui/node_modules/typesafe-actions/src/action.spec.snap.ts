import * as Types from './types';
import { action } from './action';

describe('action', () => {
  it('with type only', () => {
    const showNotification = () => action('SHOW_NOTIFICATION');
    const actual = showNotification();
    // @dts-jest:pass:snap -> Types.EmptyAction<"SHOW_NOTIFICATION">
    actual;
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
    });
  });

  it('with payload', () => {
    const showNotification = (message: string) =>
      action('SHOW_NOTIFICATION', message);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      payload: string;
    } = showNotification('Hello!');
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
      payload: 'Hello!',
    });
  });

  it('with optional payload', () => {
    const showNotification = (message?: string) =>
      action('SHOW_NOTIFICATION', message);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      payload: string | undefined;
    } = showNotification();
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
    });
  });

  it('with meta', () => {
    const showNotification = (scope: string) =>
      action('SHOW_NOTIFICATION', undefined, scope);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      meta: string;
    } = showNotification('info');
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
      meta: 'info',
    });
  });

  it('with optional meta', () => {
    const showNotification = (scope?: string) =>
      action('SHOW_NOTIFICATION', undefined, scope);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      meta: string | undefined;
    } = showNotification();
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
    });
  });

  it('with payload and meta', () => {
    const showNotification = (message: string, scope: string) =>
      action('SHOW_NOTIFICATION', message, scope);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      payload: string;
      meta: string;
    } = showNotification('Hello!', 'info');
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
      payload: 'Hello!',
      meta: 'info',
    });
  });

  it('with optional payload and meta', () => {
    const showNotification = (scope: string, message?: string) =>
      action('SHOW_NOTIFICATION', message, scope);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      payload: string | undefined;
      meta: string;
    } = showNotification('info');
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
      meta: 'info',
    });
  });

  it('with payload and optional meta', () => {
    const showNotification = (message: string, scope?: string) =>
      action('SHOW_NOTIFICATION', message, scope);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      payload: string;
      meta: string | undefined;
    } = showNotification('Hello!');
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
      payload: 'Hello!',
    });
  });

  it('with optional payload and optional meta', () => {
    const showNotification = (message?: string, scope?: string) =>
      action('SHOW_NOTIFICATION', message, scope);
    const actual: {
      type: 'SHOW_NOTIFICATION';
      payload: string | undefined;
      meta: string | undefined;
    } = showNotification();
    expect(actual).toEqual({
      type: 'SHOW_NOTIFICATION',
    });
  });
});
