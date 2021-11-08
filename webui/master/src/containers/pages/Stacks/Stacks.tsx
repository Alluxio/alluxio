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

import React from 'react';

export class Stacks extends React.Component {
  public render(): JSX.Element {
    const path = 'http://' + location.host + location.pathname;
    return (
      <div className="stacks-page" style={{ backgroundColor: 'white', minHeight: window.innerHeight }}>
        <iframe style={{ width: '100%', height: window.innerHeight, overflow: 'visible' }} src={path} />
      </div>
    );
  }
}
export default Stacks as typeof React.Component;
