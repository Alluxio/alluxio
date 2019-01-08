import axios from 'axios';
import {call, put} from 'redux-saga/effects';
import {ActionType} from 'typesafe-actions';

const performRequest = (axiosFunctionName: string, endpoint: string, payload: any) => axios[axiosFunctionName](endpoint, payload)
  .then((response: any) => ({response}))
  .catch((error: any) => ({error}));

export const getSagaRequest = (AxiosFunctionName: string, endpoint: string, successFunction: ActionType<any>, errorFunction: ActionType<any>) => function* (params: any) {
  let apiEndpoint = endpoint;

  if (params && params.payload) {
    if (params.payload.pathSuffix) {
      apiEndpoint += `/${params.payload.pathSuffix}`;
    }

    if (params.payload.queryString) {
      const queryString = Object.keys(params.payload.queryString).map(key => key + '=' + params.payload.queryString[key]).join('&');
      apiEndpoint += `?${queryString}`;
    }
  }

  try {
    const response = yield call(performRequest, AxiosFunctionName, apiEndpoint, params.payload || {});
    if (response.error) {
      yield put(errorFunction(response.error.response));
    } else {
      yield put(successFunction(response.response));
    }
  } catch (err) {
    if (err instanceof Error) {
      yield put(errorFunction(err.stack!));
    } else {
      yield put(errorFunction('An unknown fetch error occurred in versions.'));
    }
  }
};
