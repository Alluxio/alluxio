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
import {all, fork, takeLatest} from 'redux-saga/effects';

import {getSagaRequest} from '../../utilities';
import {fetchError, fetchSuccess} from './actions';
import {LogsActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_logs`;

const watchRequest = function* () {
  yield takeLatest(LogsActionTypes.FETCH_REQUEST, getSagaRequest(axios.get, API_ENDPOINT, fetchSuccess, fetchError));
};

export const logsSaga = function* () {
  yield all([fork(watchRequest)]);
};
