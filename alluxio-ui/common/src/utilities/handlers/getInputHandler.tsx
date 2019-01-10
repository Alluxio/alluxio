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

import {FieldValidatorFunctionsType, getInputValidator} from './getInputValidator';

export type InputValidatorFunctionType = (this: any, event: React.FormEvent<HTMLInputElement>) => void;

export const getInputHandler = (elementId: string, elementName: string, fieldValidatorFunctions: FieldValidatorFunctionsType) => {
  let inputValidatorFunction: InputValidatorFunctionType;
  if (fieldValidatorFunctions.length) {
    inputValidatorFunction = getInputValidator(elementId, elementName, fieldValidatorFunctions);
  }

  return function(this: any, event: React.FormEvent<HTMLInputElement>) {
    const element = event.currentTarget;
    const elementValue = element.value;
    if (inputValidatorFunction) {
      inputValidatorFunction.call(this, event);
    }
    this.setState({[elementId]: elementValue});
  }
};
