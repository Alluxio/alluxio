import {all, fork, takeLatest} from 'redux-saga/effects';

import {getSagaFetchGenerator} from '@alluxio/common-ui/src/utilities';
import {fetchError, fetchSuccess} from './actions';
import {OverviewActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_overview`;

const watchRequest = function* () {
  yield takeLatest(OverviewActionTypes.FETCH_REQUEST, getSagaFetchGenerator(API_ENDPOINT, fetchSuccess, fetchError));
};

export const overviewSaga = function* () {
  yield all([fork(watchRequest)]);
};
