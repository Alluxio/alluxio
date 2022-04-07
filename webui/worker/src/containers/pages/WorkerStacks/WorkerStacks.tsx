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

import { IApplicationState } from '../../../store';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import Stacks, {
  IStackPropsFromState,
  mapDispatchToStacksProps,
} from '@alluxio/common-ui/src/components/Stacks/Stacks';
import { connect } from 'react-redux';

const mapStateToProps = ({ init, refresh, stacks }: IApplicationState): IStackPropsFromState => ({
  errors: createAlertErrors(init.errors !== undefined || stacks.errors !== undefined),
  loading: init.loading || stacks.loading,
  refresh: refresh.data,
  stackData: stacks.data,
  class: 'stacks-page',
});

export default connect(
  mapStateToProps,
  mapDispatchToStacksProps,
)(Stacks);
