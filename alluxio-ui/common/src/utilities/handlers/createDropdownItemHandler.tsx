import React from 'react';

import {createDropdownItemValidator, FieldValidatorFunctionsType} from './createDropdownItemValidator';

type DropdownValidatorFunctionType = (this: any, event: React.FormEvent<HTMLElement>, isFormSubmission: boolean) => void;

export const createDropdownItemHandler = (elementId: string, elementName: string, fieldValidatorFunctions: FieldValidatorFunctionsType) => {
  let inputValidatorFunction: DropdownValidatorFunctionType;
  if (fieldValidatorFunctions.length) {
    inputValidatorFunction = createDropdownItemValidator(elementId, elementName, fieldValidatorFunctions);
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
