import {all, fork, takeLatest} from 'redux-saga/effects';

import {createSagaFetchGenerator} from '@alluxio/common-ui/src/utilities';
import {fetchError, fetchSuccess} from './actions';
import {WorkersActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_workers`;

const watchRequest = function* () {
  yield takeLatest(WorkersActionTypes.FETCH_REQUEST, createSagaFetchGenerator(API_ENDPOINT, fetchSuccess, fetchError));
};

export const workersSaga = function* () {
  yield all([fork(watchRequest)]);
};
