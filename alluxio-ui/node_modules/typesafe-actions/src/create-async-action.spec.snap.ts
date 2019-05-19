import * as Types from './types';
import { createAsyncAction } from './create-async-action';

type Todo = { firstName: string; lastName: string };
describe('createAsyncAction', () => {
  it('should create an async action with types', () => {
    // NOTE: with `void` type you can make it explicit that no arguments are accepted by the action-creator function
    const fetchTodos = createAsyncAction(
      'FETCH_TODOS_REQUEST',
      'FETCH_TODOS_SUCCESS',
      'FETCH_TODOS_FAILURE'
    )<void, Todo[], Error>();

    const requestResult = fetchTodos.request();
    // @dts-jest:pass:snap -> Types.EmptyAction<"FETCH_TODOS_REQUEST">
    requestResult;
    expect(requestResult).toEqual({ type: 'FETCH_TODOS_REQUEST' });
    const successResult = fetchTodos.success([
      {
        firstName: 'Piotr',
        lastName: 'Witek',
      },
    ]);
    // @dts-jest:pass:snap -> Types.PayloadAction<"FETCH_TODOS_SUCCESS", Todo[]>
    successResult;
    expect(successResult).toEqual({
      type: 'FETCH_TODOS_SUCCESS',
      payload: [
        {
          firstName: 'Piotr',
          lastName: 'Witek',
        },
      ],
    });
    const failureResult = fetchTodos.failure(Error('reason'));
    // @dts-jest:pass:snap -> Types.PayloadAction<"FETCH_TODOS_FAILURE", Error>
    failureResult;
    expect(failureResult).toEqual({
      type: 'FETCH_TODOS_FAILURE',
      payload: Error('reason'),
    });
  });

  // it('should create an async action with mappers', () => {
  //   const fetchUserMappers = createAsyncAction(
  //     'FETCH_USER_REQUEST',
  //     'FETCH_USER_SUCCESS',
  //     'FETCH_USER_FAILURE'
  //   ).withMappers(
  //     (id: string) => ({ id }), // request mapper
  //     ({ firstName, lastName }: User) => `${firstName} ${lastName}`, // success mapper
  //     () => 'hardcoded error' // error mapper
  //   );

  //   const requestResult: {
  //     type: 'FETCH_USER_REQUEST';
  //     payload: { id: string };
  //   } = fetchUserMappers.request('fake_id');
  //   expect(requestResult).toEqual({ type: 'FETCH_USER_REQUEST', payload: { id: 'fake_id' } });
  //   const successResult: {
  //     type: 'FETCH_USER_SUCCESS';
  //     payload: string;
  //   } = fetchUserMappers.success({
  //     firstName: 'Piotr',
  //     lastName: 'Witek',
  //   });
  //   expect(successResult).toEqual({
  //     type: 'FETCH_USER_SUCCESS',
  //     payload: 'Piotr Witek',
  //   });
  //   const failureResult: {
  //     type: 'FETCH_USER_FAILURE';
  //     payload: string;
  //   } = fetchUserMappers.failure();
  //   expect(failureResult).toEqual({
  //     type: 'FETCH_USER_FAILURE',
  //     payload: 'hardcoded error',
  //   });
  // });

  // it('should create an async action with unions', () => {
  //   const fetchUserMappers = createAsyncAction(
  //     'FETCH_USER_REQUEST',
  //     'FETCH_USER_SUCCESS',
  //     'FETCH_USER_FAILURE'
  //   ).withMappers(
  //     (id: string | number) => ({ id }), // request mapper
  //     ({ firstName, lastName }: User | undefined) => `${firstName} ${lastName}`, // success mapper
  //     (error: boolean | string) => error // error mapper
  //   );

  //   const requestResult: {
  //     type: 'FETCH_USER_REQUEST';
  //     payload: { id: string | number };
  //   } = fetchUserMappers.request(2);
  //   expect(requestResult).toEqual({ type: 'FETCH_USER_REQUEST', payload: { id: 2 } });
  //   const successResult: {
  //     type: 'FETCH_USER_SUCCESS';
  //     payload: string | undefined;
  //   } = fetchUserMappers.success(undefined);
  //   expect(successResult).toEqual({
  //     type: 'FETCH_USER_SUCCESS',
  //     payload: undefined,
  //   });
  //   const failureResult: {
  //     type: 'FETCH_USER_FAILURE';
  //     payload: boolean | string;
  //   } = fetchUserMappers.failure(true);
  //   expect(failureResult).toEqual({
  //     type: 'FETCH_USER_FAILURE',
  //     payload: true,
  //   });
  // });
});

// NEW API
// const getTodoAsync = createAsyncAction(
//   'GET_TODO_REQUEST',
//   'GET_TODO_SUCCESS',
//   'GET_TODO_FAILURE'
// )<{ token: string; id: string }, Todo, Error>();

// WITH MAP (PROPOSAL):
// const getTodoAsyncMap = createAsyncAction(
//   'GET_TODO_REQUEST',
//   'GET_TODO_SUCCESS',
//   'GET_TODO_FAILURE'
// ).map(...);
