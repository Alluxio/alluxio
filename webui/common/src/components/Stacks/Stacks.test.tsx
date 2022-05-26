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
import { AllStacksProps, Stacks } from './Stacks';
import { initialState } from '../../../../master/src/store';
import { createAlertErrors } from '../../utilities';

configure({ adapter: new Adapter() });

describe('Stacks', () => {
  let history: History<LocationState>;
  let props: AllStacksProps;

  beforeAll(() => {
    history = createBrowserHistory({ keyLength: 0 });
    history.push('/stacks');
    props = {
      location: { search: '' },
      history: history,
      fetchRequest: sinon.spy(() => {}),
      stackData: initialState.stacks.data,
      refresh: initialState.refresh.data,
      queryStringSuffix: '',
      request: {},
      loading: false,
      class: '',
      errors: createAlertErrors(false),
    };
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<Stacks {...props} />);
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
