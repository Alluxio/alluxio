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
import { IFetchDataProps, withFetchData } from './withFetchData';
import sinon, { SinonSpy } from 'sinon';

configure({ adapter: new Adapter() });

const WrappedComponent = (): JSX.Element => <div>Wrapped</div>;
const EnhancedComponent = withFetchData(WrappedComponent);

describe('withFetchData HOC', () => {
  let props: IFetchDataProps;

  describe('Shallow component', () => {
    let shallowWrapper: ShallowWrapper;

    beforeEach(() => {
      props = {
        refresh: false,
        fetchRequest: sinon.spy(() => {}),
      };
      shallowWrapper = shallow(<EnhancedComponent {...props} />);
    });

    it('Renders without crashing', () => {
      expect(shallowWrapper.length).toEqual(1);
    });

    it('Calls fetchRequest in componentWillMount', () => {
      sinon.assert.calledOnce(props.fetchRequest as SinonSpy);
    });

    it('Did not call fetchRequest in componentDidUpdate refresh=false', () => {
      shallowWrapper.setProps({ refresh: false });
      sinon.assert.calledOnce(props.fetchRequest as SinonSpy);
    });

    it('Calls fetchRequest in componentDidUpdate refresh=true', () => {
      sinon.assert.calledOnce(props.fetchRequest as SinonSpy); // componentWillMount
      shallowWrapper.setProps({ refresh: true });
      sinon.assert.calledTwice(props.fetchRequest as SinonSpy); // componentDidMount
    });

    it('Matches snapshot - renders WrappedComponent', () => {
      expect(shallowWrapper).toMatchSnapshot();
    });
  });
});
