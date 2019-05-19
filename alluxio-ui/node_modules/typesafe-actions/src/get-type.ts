import { StringType, ActionCreator, TypeMeta } from './types';

/**
 * @description get the "type literal" of a given action-creator
 */
export function getType<T extends StringType>(
  creator: ActionCreator<T> & TypeMeta<T>
): T {
  if (creator == null) {
    throw new Error('first argument is missing');
  }

  if (creator.getType == null) {
    throw new Error('first argument is not an instance of "typesafe-actions"');
  }

  return creator.getType();
}
