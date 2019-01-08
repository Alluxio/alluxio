import React from 'react';
import {RouteComponentProps, StaticContext} from 'react-router';

export const getRoutedViewRenderer = (
  Component: typeof React.Component,
  additionalProperties: { [key: string]: string | boolean }
) => (routerProps: RouteComponentProps<any, StaticContext, any>) => (
  <Component {...routerProps} {...additionalProperties}/>
);
