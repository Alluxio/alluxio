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

import {configure, mount, ReactWrapper, shallow, ShallowWrapper} from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import {createBrowserHistory, History, LocationState} from 'history';
import React from 'react';
import {Provider} from 'react-redux';
import {Store} from 'redux';
import sinon from 'sinon';

import {AllProps} from '@alluxio/common-ui/src/components';
import configureStore from '../../../configureStore'
import {initialState, IApplicationState} from '../../../store';
import ConnectedApp from '../../App/App';
import {routePaths} from "../../../constants";
import MasterLogs from "./MasterLogs";
import {createAlertErrors} from "@alluxio/common-ui/src/utilities";

configure({adapter: new Adapter()});

describe('MasterLogs', () => {
  let history: History<LocationState>;
  let store: Store<IApplicationState>;
  let props: AllProps;

  beforeAll(() => {
    history = createBrowserHistory({keyLength: 0});
    history.push(routePaths.logs);
    store = configureStore(history, initialState);
    props = {
      location: {search: ''},
      history: history,
      fetchRequest: sinon.spy(() => {}),
      data: initialState.logs.data,
      refresh: initialState.refresh.data,
      textAreaHeight: 0,
      request: {},
      updateRequestParameter: sinon.spy(() => {}),
      queryStringSuffix: '',
      errors: createAlertErrors(false),
      loading: false,
      class: ''
    };
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<MasterLogs {...props}/>);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Matches snapshot', () => {
      expect(shallowWrapper).toMatchSnapshot();
    });
  });
});
