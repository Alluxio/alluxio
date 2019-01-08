import {all, fork, takeLatest} from 'redux-saga/effects';

import {getSagaFetchGenerator} from '@alluxio/common-ui/src/utilities';
import {fetchError, fetchSuccess} from './actions';
import {ConfigActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_config`;

const watchRequest = function* () {
  yield takeLatest(ConfigActionTypes.FETCH_REQUEST, getSagaFetchGenerator(API_ENDPOINT, fetchSuccess, fetchError));
};

export const configSaga = function* () {
  yield all([fork(watchRequest)]);
};
