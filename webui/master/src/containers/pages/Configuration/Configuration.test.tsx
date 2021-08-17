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

import { AllProps, ConfigurationPresenter } from './Configuration';
import { routePaths } from '../../../constants';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { IConfig } from '../../../store/config/types';

configure({ adapter: new Adapter() });

describe('Configuration', () => {
  let history: History<LocationState>;
  let props: AllProps;

  beforeAll(() => {
    history = createBrowserHistory({ keyLength: 0 });
    history.push(routePaths.config);

    const testData: IConfig = { configuration: [{ left: 'alluxio.', middle: '', right: '' }], whitelist: ['/'] };

    props = {
      data: testData,
      class: '',
      errors: createAlertErrors(false),
      loading: false,
      fetchRequest: sinon.spy(() => {}),
      refresh: false,
    };
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<ConfigurationPresenter {...props} />);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Matches snapshot', () => {
      expect(shallowWrapper).toMatchSnapshot();
    });

    it('Searches data with valid search key', () => {
      shallowWrapper.setState({ searchConfig: 'alluxio.' });
      expect(
        shallowWrapper
          .find('Table')
          .first()
          .dive()
          .find('#filtered-data-body'),
      ).toHaveLength(1);
    });

    it('Searches data with invalid search key', () => {
      const randomStr = Math.random()
        .toString(36)
        .substring(7);
      shallowWrapper.setState({ searchConfig: randomStr });
      expect(
        shallowWrapper
          .find('Table')
          .first()
          .dive()
          .find('#no-data-body'),
      ).toHaveLength(1);
    });
  });
});
