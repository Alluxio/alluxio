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

import 'babel-polyfill';
import 'raf/polyfill';
import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';

import 'source-sans-pro/source-sans-pro.css';
import 'source-serif-pro/source-serif-pro.css';
import '@openfonts/anonymous-pro_all';

import './index.css';

ReactDOM.render(<App />, document.getElementById('root') as HTMLElement);
