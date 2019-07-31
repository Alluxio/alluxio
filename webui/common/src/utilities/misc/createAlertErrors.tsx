import {IAlertErrors} from "../../constants";

export function createAlertErrors(errorCondition: boolean, errorList?: string[]): IAlertErrors {
    const errors: IAlertErrors = {
        hasErrors: errorCondition,
        general: errorCondition,
        specific: []
    };

    errorList && errorList.forEach(err => err && (errors.hasErrors = true) && errors.specific.push(err));

    return errors;
}