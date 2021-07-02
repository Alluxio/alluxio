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
import { AllProps, BlockInfoPresenter } from './BlockInfo';
import { routePaths } from '../../../constants';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';

configure({ adapter: new Adapter() });

describe('BlockInfo', () => {
  let history: History<LocationState>;
  let props: AllProps;

  beforeAll(() => {
    history = createBrowserHistory({ keyLength: 0 });
    history.push(routePaths.blockInfo);
    props = {
      refresh: false,
      fetchRequest: sinon.spy(() => {}),
      data: initialState.blockInfo.data,
      loading: false,
      errors: createAlertErrors(false),
      request: {},
      class: '',
    };
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<BlockInfoPresenter {...props} />);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Contains a div with class col-12', () => {
      expect(shallowWrapper.find('.col-12').length).toEqual(1);
    });

    describe('Renders BlockInfo Listing', () => {
      it('Matches snapshot with Table listing', () => {
        expect(shallowWrapper).toMatchSnapshot();
      });
    });

    describe('Renders BlockInfo view', () => {
      beforeAll(() => {
        const data = { ...props.data };
        data.blockSizeBytes = 'x';
        shallowWrapper.setProps({ data: data });
      });

      it('Matches snapshot with File', () => {
        expect(shallowWrapper).toMatchSnapshot();
      });
    });
  });
});
