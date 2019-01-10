/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

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
      const queryString = Object.keys(params.payload.queryString)
        .filter(key => params.payload.queryString[key] !== undefined)
        .map(key => key + '=' + encodeURIComponent(params.payload.queryString[key]))
        .join('&');
      apiEndpoint += queryString.length ? `?${queryString}` : '';
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
