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
import { Button, PopoverBody, UncontrolledPopover } from 'reactstrap';
import slackLogo from '../../images/icon-slack.svg';
import './SlackButton.css';

const SLACK_ID = 'SlackChannel';
const SLACK_URL = 'https://www.alluxio.io/slack';

export class SlackButton extends React.PureComponent {
  public render(): JSX.Element {
    return (
      <>
        <div id={SLACK_ID} className="absolute-bottom-right">
          <Button
            id={SLACK_ID}
            className="button-icon"
            color="info"
            onClick={(): void => {
              window.open(SLACK_URL, '_blank');
            }}
          >
            <img src={slackLogo} />
          </Button>
        </div>
        <UncontrolledPopover placement="auto" target={SLACK_ID} trigger="hover">
          <PopoverBody>
            <span>
              Ask a question on{' '}
              <a href={SLACK_URL} rel="noopener noreferrer" target="_blank">
                Slack
              </a>
              !
            </span>
          </PopoverBody>
        </UncontrolledPopover>
      </>
    );
  }
}
