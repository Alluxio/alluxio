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
