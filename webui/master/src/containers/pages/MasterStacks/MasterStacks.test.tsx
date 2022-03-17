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

import { createBrowserHistory, History, LocationState } from 'history';
import { configure, shallow, ShallowWrapper } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import React from 'react';
import sinon from 'sinon';

import MasterStacks from './MasterStacks';
import { AllStacksProps } from '@alluxio/common-ui/src/components/Stacks/Stacks';
import { routePaths } from '../../../constants';
import { initialState } from '../../../store';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';

configure({ adapter: new Adapter() });

describe('Master Stacks', () => {
  let history: History<LocationState>;
  let props: AllStacksProps;

  beforeAll(() => {
    history = createBrowserHistory({ keyLength: 0 });
    history.push(routePaths.stacks);
    props = {
      location: { search: '' },
      history: history,
      fetchRequest: sinon.spy(() => {}),
      stackData: initialState.stacks.data,
      refresh: initialState.refresh.data,
      request: {},
      queryStringSuffix: '',
      errors: createAlertErrors(false),
      loading: false,
      class: '',
    };
  });

  afterEach(() => {
    sinon.restore();
  });
  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<MasterStacks {...props} />);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Matches snapshot', () => {
      expect(shallowWrapper).toMatchSnapshot();
    });
  });
});
