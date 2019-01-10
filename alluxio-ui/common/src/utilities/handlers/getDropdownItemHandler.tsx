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

import {FieldValidatorFunctionsType, getDropdownItemValidator} from './getDropdownItemValidator';

type DropdownValidatorFunctionType = (this: any, event: React.FormEvent<HTMLElement>, isFormSubmission: boolean) => void;

export const getDropdownItemHandler = (elementId: string, elementName: string, fieldValidatorFunctions: FieldValidatorFunctionsType) => {
  let inputValidatorFunction: DropdownValidatorFunctionType;
  if (fieldValidatorFunctions.length) {
    inputValidatorFunction = getDropdownItemValidator(elementId, elementName, fieldValidatorFunctions);
  }

  return function(this: any, event: React.MouseEvent<HTMLElement>, isFormSubmission?: boolean) {
    const element = event.currentTarget;
    const elementValue = element && element.getAttribute('dropdownvalue');
    if (inputValidatorFunction) {
      inputValidatorFunction.call(this, event, isFormSubmission);
    }
    this.setState({[elementId]: elementValue});
  }
};
