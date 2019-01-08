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
