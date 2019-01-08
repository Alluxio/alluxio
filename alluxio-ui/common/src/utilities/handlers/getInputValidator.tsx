import React from 'react';

export type FieldValidatorFunctionType = (name: string, value: string) => string;
export type FieldValidatorFunctionsType = FieldValidatorFunctionType[];

export const getInputValidator = (elementId: string, elementName: string, fieldValidatorFunctions: FieldValidatorFunctionsType) =>
  function (this: any, event: React.FormEvent<HTMLInputElement>) {
    const element = document.getElementById(elementId);
    const elementValue = element && element.getAttribute('value') || '';
    const errors = fieldValidatorFunctions.map((validator: FieldValidatorFunctionType) => validator(elementName, elementValue)).filter((value: string) => value !== '');
    this.setState((prevState: any) => {
      if (errors.length) {
        return {validationErrors: {...prevState.validationErrors, ...{[elementId]: errors}}};
      }

      const newState = {...prevState};
      delete newState.validationErrors[elementId];
      return newState;
    });
  };

