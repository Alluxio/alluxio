import axios from 'axios';
import {call, put} from 'redux-saga/effects';
import {ActionType} from 'typesafe-actions';

export const createSagaPostGenerator = (endpoint: string, successFunction: ActionType<any>, errorFunction: ActionType<any>) => function* (params: any) {
  let apiEndpoint = endpoint;

  if (params && params.payload) {
    if (params.payload.pathSuffix) {
      apiEndpoint += `/${params.payload.pathSuffix}`;
    }

    if (params.payload.queryString) {
      const queryString = Object.keys(params.payload.queryString)
        .filter(key => params.payload.queryString[key] !== undefined)
        .map(key => key + '=' + encodeURIComponent(params.payload.queryString[key]))
        .join('&');
      apiEndpoint += queryString.length ? `?${queryString}` : '';
    }
  }

  try {
    const response = yield call(axios.post, apiEndpoint, params.payload || {});

    if (response.error) {
      yield put(errorFunction(response.error));
    } else {
      yield put(successFunction(response.data));
    }
  } catch (err) {
    if (err instanceof Error) {
      yield put(errorFunction(err.stack!));
    } else {
      yield put(errorFunction('An unknown fetch error occurred in versions.'));
    }
  }
};
