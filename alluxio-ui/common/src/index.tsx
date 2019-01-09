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

import 'raf/polyfill';
import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';

// Order matters for the following files, so disable alphabetization
// tslint:disable:ordered-imports
import 'source-sans-pro/source-sans-pro.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap/dist/css/bootstrap-grid.min.css';
import 'bootstrap/dist/css/bootstrap-reboot.min.css';
// tslint:enable:ordered-imports

ReactDOM.render(
  <App />,
  document.getElementById('root') as HTMLElement
);
