import {all, fork, takeLatest} from 'redux-saga/effects';

import {getSagaFetchGenerator} from '@alluxio/common-ui/src/utilities';
import {fetchError, fetchSuccess} from './actions';
import {BlockInfoActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_blockinfo`;

const watchRequest = function* () {
  yield takeLatest(BlockInfoActionTypes.FETCH_REQUEST, getSagaFetchGenerator(API_ENDPOINT, fetchSuccess, fetchError));
};

export const blockInfoSaga = function* () {
  yield all([fork(watchRequest)]);
};
