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

import logo from '../../images/alluxio-mark-tight-sm.svg';

import './LoadingMessage.css';

export class LoadingMessage extends React.PureComponent {
  public render() {
    return (
      <div className="loadingMessageWrapper">
        <div className="loadingMessage text-center">
          <img className="loadingMessageSpin" src={logo}/>
          <br/>
          <h5 className="">Please wait...</h5>
        </div>
      </div>
    );
  }
}

export default LoadingMessage;
