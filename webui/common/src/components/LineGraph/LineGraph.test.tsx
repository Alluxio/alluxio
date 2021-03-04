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

import { ILineGraphProps, LineGraph } from './LineGraph';

configure({ adapter: new Adapter() });

describe('Paginator', () => {
  let props: ILineGraphProps;

  beforeAll(() => {
    props = {
      data: [{ id: '', data: [] }],
      yAxisLabel: '',
      xAxisLabel: '',
      xAxisUnits: '',
      yAxisUnits: '',
    };
  });

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeEach(() => {
      shallowWrapper = shallow(<LineGraph {...props} />);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Matches percent y-axis snapshot', () => {
      shallowWrapper.setProps({
        data: [
          {
            id: '% Test',
            data: [{ x: 1614803627951, y: 0 }],
          },
        ],
        yAxisLabel: 'Percent (%)',
        xAxisLabel: 'Time Stamp',
      });
      expect(shallowWrapper).toMatchSnapshot();
    });

    it('Matches throughput y-axis snapshot', () => {
      shallowWrapper.setProps({
        data: [
          {
            id: 'Test',
            data: [{ x: 1614803627951, y: 0 }],
          },
        ],
        yAxisLabel: 'Raw Throughput (Bytes/Minute)',
        xAxisLabel: 'Time Stamp',
      });
      expect(shallowWrapper).toMatchSnapshot();
    });
  });
});
