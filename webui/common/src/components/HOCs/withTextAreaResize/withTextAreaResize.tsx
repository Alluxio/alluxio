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
import {getDebouncedFunction, getDisplayName} from "../../../utilities";

export interface ITextAreaResizeState {
    textAreaHeight?: number;
}

export function withTextAreaResize<T>(WrappedComponent: React.ComponentType<T>) {
    class textAreaResizeHoc extends React.Component<T, ITextAreaResizeState> {
        private readonly textAreaResizeMs = 100;
        private readonly debouncedUpdateTextAreaHeight = getDebouncedFunction(this.updateTextAreaHeight.bind(this), this.textAreaResizeMs, true);

        constructor(props: T) {
            super(props);

            this.updateTextAreaHeight = this.updateTextAreaHeight.bind(this);
        }

        public componentWillMount() {
            this.updateTextAreaHeight();
        }

        public componentDidMount() {
            window.addEventListener('resize', this.debouncedUpdateTextAreaHeight);
        }

        public componentWillUnmount() {
            window.removeEventListener('resize', this.debouncedUpdateTextAreaHeight);
        }

        public render() {
            return <WrappedComponent {...this.props} textAreaHeight={this.state.textAreaHeight} />;
        }

        private updateTextAreaHeight() {
            this.setState({textAreaHeight: window.innerHeight / 2});
        }
    }
    (textAreaResizeHoc as React.ComponentType<any>).displayName = `withTextAreaResize(${getDisplayName(WrappedComponent)})`;
    return textAreaResizeHoc;
}
