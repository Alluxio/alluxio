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

import { createEncodedQueryString, parseQueryString } from './queryStringUtils';

describe('queryStringUtils', () => {
  describe('createEncodedQueryString', () => {
    it('single parameter', () => {
      expect(
        createEncodedQueryString({
          name: 'hello',
        }),
      ).toEqual('?name=hello');
    });
    it('multiple parameters', () => {
      expect(
        createEncodedQueryString({
          name: 'hello',
          obj: '56',
        }),
      ).toEqual('?name=hello&obj=56');
    });
    it('parameter with equal sign (=) in value', () => {
      expect(
        createEncodedQueryString({
          path: 'test/user/path/db=2323',
        }),
      ).toEqual('?path=test%2Fuser%2Fpath%2Fdb%3D2323');
    });
    it('mix parameters', () => {
      expect(
        createEncodedQueryString({
          name: 'hello',
          obj: '56',
          path: 'test/user/path/db=2323',
        }),
      ).toEqual('?name=hello&obj=56&path=test%2Fuser%2Fpath%2Fdb%3D2323');
    });
  });
  describe('parseQueryString', () => {
    it('single parameter', () => {
      expect(parseQueryString('?name=hello')).toEqual({
        name: 'hello',
      });
    });
    it('multiple parameters', () => {
      expect(parseQueryString('?name=hello&obj=56')).toEqual({
        name: 'hello',
        obj: '56',
      });
    });
    it('parameter with equal sign (=) in value', () => {
      expect(parseQueryString('?path=test/user/path/db=2323')).toEqual({
        path: 'test/user/path/db=2323',
      });
    });
    it('mix parameters', () => {
      expect(parseQueryString('?name=hello&obj=56&path=test/user/path/db=2323')).toEqual({
        name: 'hello',
        obj: '56',
        path: 'test/user/path/db=2323',
      });
    });
    it('ignore trailing &', () => {
      expect(parseQueryString('?name=hello&')).toEqual({
        name: 'hello',
      });
    });
    it('trailing = in value', () => {
      expect(parseQueryString('?name=hello=')).toEqual({
        name: 'hello=',
      });
    });
    it('empty parameters', () => {
      expect(parseQueryString('?=&')).toEqual({});
      expect(parseQueryString('?&=')).toEqual({});
      expect(parseQueryString('&?=')).toEqual({});
      expect(parseQueryString('&=?')).toEqual({});
      expect(parseQueryString('=&?')).toEqual({});
      expect(parseQueryString('=?&')).toEqual({});
    });
    it('throw if parameter with missing value', () => {
      expect(() => parseQueryString('?name=')).toThrow('unable to parse querystring');
    });
  });
});
