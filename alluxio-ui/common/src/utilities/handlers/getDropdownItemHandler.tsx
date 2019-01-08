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
