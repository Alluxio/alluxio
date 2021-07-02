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
import React from 'react';
import { IErrorProps, withErrors } from './withErrors';
import { createAlertErrors } from '../../../utilities';

configure({ adapter: new Adapter() });

const WrappedComponent = (): JSX.Element => <div>Wrapped</div>;
const EnhancedComponent = withErrors(WrappedComponent);

describe('withErrors HOC', () => {
  let props: IErrorProps;

  beforeAll(() => {
    props = {
      errors: createAlertErrors(false),
    };
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<EnhancedComponent {...props} />);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    describe('No Errors', () => {
      it('Matches snapshot - renders WrappedComponent', () => {
        shallowWrapper.setProps({ errors: createAlertErrors(false) });
        expect(shallowWrapper).toMatchSnapshot();
      });
    });

    describe('With Errors', () => {
      it('Matches snapshot - renders general error', () => {
        shallowWrapper.setProps({ errors: createAlertErrors(true) });
        expect(shallowWrapper).toMatchSnapshot();
      });

      it('Matches snapshot - renders specific errors', () => {
        shallowWrapper.setProps({ errors: createAlertErrors(true, ['error1', 'error2']) });
        expect(shallowWrapper).toMatchSnapshot();
      });
    });
  });
});
