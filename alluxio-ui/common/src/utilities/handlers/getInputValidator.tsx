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

