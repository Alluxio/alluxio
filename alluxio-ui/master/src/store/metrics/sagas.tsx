import {all, fork, takeLatest} from 'redux-saga/effects';

import {getSagaFetchGenerator} from '@alluxio/common-ui/src/utilities';
import {fetchError, fetchSuccess} from './actions';
import {MetricsActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_metrics`;

const watchRequest = function* () {
  yield takeLatest(MetricsActionTypes.FETCH_REQUEST, getSagaFetchGenerator(API_ENDPOINT, fetchSuccess, fetchError));
};

export const metricsSaga = function* () {
  yield all([fork(watchRequest)]);
};
