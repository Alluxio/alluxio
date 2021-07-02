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

import { Theme } from '@nivo/core';
import { LineSerieData, ResponsiveLine } from '@nivo/line';
import React from 'react';

import './LineGraph.css';

export interface ILineGraphProps {
  data: LineSerieData[];
  xAxisLabel: string;
  xAxisUnits: string;
  yAxisLabel: string;
  yAxisUnits: string;
}

export class LineGraph extends React.PureComponent<ILineGraphProps> {
  public render(): JSX.Element {
    const { data, xAxisLabel, yAxisLabel } = this.props;
    const nivoTheme: Theme = {
      background: 'transparent',
      axis: {
        domain: { line: { stroke: 'transparent', strokeWidth: 1 } },
        ticks: { line: { stroke: '#fff', strokeWidth: 1 }, text: { fill: '#bbb', fontSize: 12 } },
        legend: { text: { fill: '#bbb', fontSize: 12 } },
      },
      grid: { line: { stroke: '#ddd', strokeWidth: 1 } },
      legends: { text: { fill: '#bbb', fontSize: 12 } },
      labels: { text: { fill: '#bbb', fontSize: 12 } },
      markers: { lineColor: '#fff', lineStrokeWidth: 1, textColor: '#999', fontSize: '12' },
      dots: { text: { fill: '#bbb', fontSize: 12 } },
      tooltip: {
        container: {
          background: '#222',
          color: 'inherit',
          fontSize: 'inherit',
          borderRadius: '2px',
          boxShadow: '0 1px 2px rgba(255, 255, 255, 0.25)',
          padding: '5px 9px',
        },
        basic: { whiteSpace: 'pre', display: 'flex', alignItems: 'center' },
        table: {},
        tableCell: { padding: '3px 5px' },
      },
    };

    return (
      <div className="lineGraph col-12 col-md-6 col-xl-4">
        <h5>
          {data[0].id}
          {!yAxisLabel.includes('%') && ' (Bytes/Minute)'}
        </h5>
        <ResponsiveLine
          theme={nivoTheme}
          data={data}
          margin={{ top: 25, right: 50, bottom: 200, left: 75 }}
          xScale={{ type: 'time', format: '%Q', precision: 'minute' }}
          yScale={{
            type: 'linear',
            stacked: false,
            min: yAxisLabel.includes('%') ? 0 : 'auto',
            max: yAxisLabel.includes('%') ? 100 : 'auto',
          }}
          axisTop={null}
          axisRight={null}
          axisBottom={{
            tickSize: 5,
            tickPadding: 5,
            tickRotation: -90,
            legend: xAxisLabel,
            legendOffset: 140,
            legendPosition: 'middle',
            format: '%x %H:%M:%S',
          }}
          axisLeft={{
            tickSize: 5,
            tickPadding: 5,
            tickRotation: 0,
            legend: yAxisLabel.includes('%') ? yAxisLabel : undefined,
            legendOffset: -40,
            legendPosition: 'middle',
          }}
          dotSize={8}
          dotColor="inherit:darker(0.3)"
          dotBorderWidth={1}
          dotBorderColor="#fff"
          enableDotLabel={true}
          animate={true}
          motionStiffness={90}
          motionDamping={15}
          curve="natural"
          colors="set3"
          enableArea={true}
          areaOpacity={0.5}
          areaBaselineValue={0}
          isInteractive={true}
          enableStackTooltip={true}
        />
      </div>
    );
  }
}
