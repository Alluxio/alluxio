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
import { ILoadingProps, withLoadingMessage } from './withLoadingMessage';

configure({ adapter: new Adapter() });

const WrappedComponent = (): JSX.Element => <div>Wrapped</div>;
const EnhancedComponent = withLoadingMessage(WrappedComponent);

describe('withLoadingMessage HOC', () => {
  let props: ILoadingProps;

  beforeAll(() => {
    props = {
      loading: false,
      class: 'test',
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

    describe('Loading', () => {
      beforeAll(() => {
        shallowWrapper.setProps({ loading: true });
      });

      it('Contains a div with class test', () => {
        expect(shallowWrapper.find('.test').length).toEqual(1);
      });

      it('Matches snapshot - renders LoadingMessage', () => {
        expect(shallowWrapper).toMatchSnapshot();
      });
    });

    describe('Not Loading', () => {
      it('Matches snapshot - renders WrappedComponent', () => {
        shallowWrapper.setProps({ loading: false });
        expect(shallowWrapper).toMatchSnapshot();
      });
    });
  });
});
