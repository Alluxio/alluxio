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

import {configure, shallow, ShallowWrapper} from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import {createBrowserHistory, History, LocationState} from 'history';
import React from 'react';
import sinon from 'sinon';
import Logs, {AllProps, LogsPresenter} from './Logs';
import {initialState} from "../../../../master/src/store";
import {createAlertErrors} from "../../utilities";


configure({adapter: new Adapter()});

describe('Logs', () => {
  let history: History<LocationState>;
  let props: AllProps;

  beforeAll(() => {
    history = createBrowserHistory({keyLength: 0});
    history.push('/logs');
    props = {
      location: {search: ''},
      history: history,
      fetchRequest: sinon.spy(() => {}),
      data: initialState.logs.data,
      refresh: initialState.refresh.data,
      textAreaHeight: 0,
      queryStringSuffix: '',
      request: {},
      loading: false,
      class: '',
      errors: createAlertErrors(false),
      updateRequestParameter: sinon.spy()
    };
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<LogsPresenter {...props}/>);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Contains a div with class col-12', () => {
      expect(shallowWrapper.find('.col-12').length).toEqual(1);
    });

    describe('Renders Directory Listing', () => {
      it('Matches snapshot with Table listing', () => {
        expect(shallowWrapper).toMatchSnapshot();
      });
    });

    describe('Renders FileView', () => {
      beforeAll(() => {
        const data = {...props.data};
        data.fileData = null;
        shallowWrapper.setProps({data: data});
      });

      it('Matches snapshot with File', () => {
        expect(shallowWrapper).toMatchSnapshot();
      });
    });
  });
});
