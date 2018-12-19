export type IScopedPropertyInfo = {
  [scopeKey in 'MASTER' | 'WORKER' | 'CLIENT' | 'SERVER' | 'ALL' | 'NONE']: {// This is derived from 'alluxio/core/common/src/main/java/alluxio/wire/Scope.java'
    [propertyKey: string]: string; // we allow any string as a property key here
  };
};
