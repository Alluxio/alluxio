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
import React from 'react';
import {StaticRouter} from 'react-router';

import {Footer, IFooterProps} from './Footer';

configure({adapter: new Adapter()});

describe('Footer', () => {
  let props: IFooterProps;

  beforeAll(() => {
    props = {
      data: []
    };
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<Footer {...props}/>);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Matches snapshot', () => {
      expect(shallowWrapper).toMatchSnapshot();
    });
  });

  describe('React component', () => {
    let reactWrapper: ReactWrapper;
    let context = {};

    beforeAll(() => {
      reactWrapper = mount(
        <StaticRouter location="someLocation" context={context}>
          <Footer {...props}/>
        </StaticRouter>
      );
    });

    it('Renders without crashing', () => {
      expect(reactWrapper.length).toEqual(1);
    });

    it('Matches snapshot', () => {
      expect(reactWrapper).toMatchSnapshot();
    });
  });
});
