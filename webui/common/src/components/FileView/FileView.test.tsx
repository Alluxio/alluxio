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

import { configure, mount, ReactWrapper, shallow, ShallowWrapper } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import { createBrowserHistory, History, LocationState } from 'history';
import React from 'react';
import { StaticRouter } from 'react-router';
import sinon from 'sinon';

import { FileView, IFileViewProps } from './FileView';

configure({ adapter: new Adapter() });

describe('FileView', () => {
  let props: IFileViewProps;
  let history: History<LocationState>;

  beforeAll(() => {
    history = createBrowserHistory({ keyLength: 0 });
    props = {
      beginInputHandler: sinon.spy(() => {}),
      endInputHandler: sinon.spy(() => {}),
      history: history,
      offsetInputHandler: sinon.spy(() => {}),
      queryStringPrefix: '',
      queryStringSuffix: '',
      viewData: {
        currentPath: '',
        debug: true,
        fatalError: '',
        fileData: '',
        fileInfos: [],
        invalidPathError: '',
        ntotalFile: 0,
        viewingOffset: 0,
      },
    };
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeAll(() => {
      shallowWrapper = shallow(<FileView {...props} />);
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
    const context = {};

    beforeAll(() => {
      reactWrapper = mount(
        <StaticRouter location="someLocation" context={context}>
          <FileView {...props} />
        </StaticRouter>,
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
