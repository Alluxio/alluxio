import React from 'react';

import {createInputValidator, FieldValidatorFunctionsType} from './createInputValidator';

export type InputValidatorFunctionType = (this: any, event: React.FormEvent<HTMLInputElement>) => void;

export const createInputHandler = (elementId: string, elementName: string, fieldValidatorFunctions: FieldValidatorFunctionsType) => {
  let inputValidatorFunction: InputValidatorFunctionType;
  if (fieldValidatorFunctions.length) {
    inputValidatorFunction = createInputValidator(elementId, elementName, fieldValidatorFunctions);
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
