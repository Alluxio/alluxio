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

import { configure, shallow } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import React from 'react';
import { withTextAreaResize } from './withTextAreaResize';

configure({ adapter: new Adapter() });

const WrappedComponent = (): JSX.Element => <div>Wrapped</div>;
const EnhancedComponent = withTextAreaResize(WrappedComponent);
const defaultHeight = 100;

describe('withFetchData HOC', () => {
  describe('Shallow component', () => {
    beforeEach(() => {
      // reset window height before each test
      Object.defineProperty(window, 'innerHeight', { writable: true, configurable: true, value: defaultHeight });
    });

    it('Renders without crashing', () => {
      const shallowWrapper = shallow(<EnhancedComponent />);
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Check state does not update before debounce delay', () => {
      const shallowWrapper = shallow(<EnhancedComponent />);
      Object.defineProperty(window, 'innerHeight', { writable: true, configurable: true, value: 200 });
      expect(shallowWrapper.state('textAreaHeight')).toEqual(50);
    });

    it('Check state updates after debounce delay', () => {
      const shallowWrapper = shallow(<EnhancedComponent />);
      Object.defineProperty(window, 'innerHeight', { writable: true, configurable: true, value: 200 });
      setTimeout(() => {
        expect(shallowWrapper.state('textAreaHeight')).toEqual(100);
      }, 100);
    });

    it('Check state does not not updates after unmount', () => {
      const shallowWrapper = shallow(<EnhancedComponent />);
      shallowWrapper.unmount();
      Object.defineProperty(window, 'innerHeight', { writable: true, configurable: true, value: 500 });
      setTimeout(() => {
        expect(shallowWrapper.state('textAreaHeight')).toEqual(250);
      }, 100);
    });

    it('Matches snapshot - renders WrappedComponent', () => {
      const shallowWrapper = shallow(<EnhancedComponent />);
      expect(shallowWrapper).toMatchSnapshot();
    });
  });
});
