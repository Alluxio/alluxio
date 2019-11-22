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

import { configure, shallow, ShallowWrapper } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import { createBrowserHistory, History, LocationState } from 'history';
import React from 'react';
import sinon from 'sinon';

import { initialState } from '../../../store';
import { AllProps, DataPresenter } from './Data';
import { routePaths } from '../../../constants';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';

configure({ adapter: new Adapter() });

describe('Data', () => {
  let history: History<LocationState>;
  let props: AllProps;

  beforeAll(() => {
    history = createBrowserHistory({ keyLength: 0 });
    history.push(routePaths.data);
    props = {
      request: {},
      refresh: false,
      fetchRequest: sinon.spy(() => {}),
      data: initialState.data.data,
      errors: createAlertErrors(false),
      class: '',
      loading: false,
    };
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<DataPresenter {...props} />);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Contains a div with class col-12', () => {
      expect(shallowWrapper.find('.col-12').length).toEqual(1);
    });

    it('Matches snapshot', () => {
      expect(shallowWrapper).toMatchSnapshot();
    });
  });
});
