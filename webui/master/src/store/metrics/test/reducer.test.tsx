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

import { initialMetricsState, metricsReducer } from '../reducer';
import { MetricsActionTypes } from '../types';
import { LineSerieData } from '@nivo/line';

type ITimeSeriesMetrics = { name: string; dataPoints: { timeStamp: number; value: number }[] };

interface IPayload {
  type: MetricsActionTypes;
  payload: { data: { timeSeriesMetrics: ITimeSeriesMetrics[] } };
}

interface ITest {
  input: ITimeSeriesMetrics[];
  output: LineSerieData[];
}

const testCases: ITest[] = [
  // lt 20 datapoints
  {
    input: [
      {
        name: 'test',
        dataPoints: Array.from({ length: 1 }, (_, index) => {
          return { timeStamp: index, value: index };
        }),
      },
    ],
    output: [
      {
        data: Array.from({ length: 1 }, (_, index) => {
          return { x: index, y: index };
        }),
        id: 'test',
        xAxisLabel: 'Time Stamp',
        yAxisLabel: 'Percent (%)',
      },
    ],
  },
  // eq datapoints
  {
    input: [
      {
        name: 'test',
        dataPoints: Array.from({ length: 20 }, (_, index) => {
          return { timeStamp: index, value: index };
        }),
      },
    ],
    output: [
      {
        data: Array.from({ length: 20 }, (_, index) => {
          return { x: index, y: index };
        }),
        id: 'test',
        xAxisLabel: 'Time Stamp',
        yAxisLabel: 'Percent (%)',
      },
    ],
  },
  // gt 20 datapoints
  {
    input: [
      {
        name: 'test',
        dataPoints: Array.from({ length: 21 }, (_, index) => {
          return { timeStamp: index, value: index };
        }),
      },
    ],
    output: [
      {
        data: Array.from({ length: 21 }, (_, index) => {
          return { x: index, y: index };
        }).slice(-20),
        id: 'test',
        xAxisLabel: 'Time Stamp',
        yAxisLabel: 'Percent (%)',
      },
    ],
  },
];

describe('MetricsReducer', () => {
  const testMetricsReducer = (test: ITest): void => {
    it(`${test.input.length} datapoints`, () => {
      const p: IPayload = {
        type: MetricsActionTypes.FETCH_SUCCESS,
        payload: {
          data: {
            timeSeriesMetrics: test.input,
          },
        },
      };
      expect(metricsReducer({ ...initialMetricsState }, p).data.timeSeriesMetrics).toEqual(test.output);
    });
  };
  testCases.forEach(test => testMetricsReducer(test));
});
