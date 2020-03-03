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

import { AxiosResponse } from 'axios';
import { call, put, Effect } from 'redux-saga/effects';
import { SagaIterator } from 'redux-saga';
import { ActionType } from 'typesafe-actions';

const performRequest = (axiosMethod: Function, endpoint: string, payload: {}): Function =>
  axiosMethod(endpoint, payload)
    .then((response: AxiosResponse) => ({ response }))
    .catch((error: Error) => ({ error }));

export const getSagaRequest = (
  AxiosFunction: Function,
  endpoint: string,
  successFunction: ActionType<Effect>,
  errorFunction: ActionType<Effect>,
) =>
  function*(params: {}): SagaIterator {
    let apiEndpoint = endpoint;
    const payload = (params as { payload: { pathSuffix: string; queryString: { [key: string]: string } } }).payload;
    if (params && payload) {
      if (payload.pathSuffix) {
        apiEndpoint += `/${payload.pathSuffix}`;
      }

      if (payload.queryString) {
        const queryString = Object.keys(payload.queryString)
          .filter(key => payload.queryString[key] !== undefined)
          .map(key => key + '=' + encodeURIComponent(payload.queryString[key]))
          .join('&');
        apiEndpoint += queryString.length ? `?${queryString}` : '';
      }
    }

    try {
      const response = yield call(performRequest, AxiosFunction, apiEndpoint, payload || {});
      if (response.error) {
        yield put(errorFunction(response.error.response));
      } else {
        yield put(successFunction(response.response));
      }
    } catch (err) {
      if (err instanceof Error && err.stack !== undefined) {
        yield put(errorFunction(err.stack));
      } else {
        yield put(errorFunction('An unknown fetch error occurred in versions.'));
      }
    }
  };
