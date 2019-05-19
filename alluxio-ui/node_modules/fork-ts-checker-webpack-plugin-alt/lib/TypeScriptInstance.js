"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// TODO: keep in sync with TypeScript
var ScriptKind;
(function (ScriptKind) {
    ScriptKind[ScriptKind["Unknown"] = 0] = "Unknown";
    ScriptKind[ScriptKind["JS"] = 1] = "JS";
    ScriptKind[ScriptKind["JSX"] = 2] = "JSX";
    ScriptKind[ScriptKind["TS"] = 3] = "TS";
    ScriptKind[ScriptKind["TSX"] = 4] = "TSX";
    ScriptKind[ScriptKind["External"] = 5] = "External";
    ScriptKind[ScriptKind["JSON"] = 6] = "JSON";
    /**
     * Used on extensions that doesn't define the ScriptKind but the content defines it.
     * Deferred extensions are going to be included in all project contexts.
     */
    ScriptKind[ScriptKind["Deferred"] = 7] = "Deferred";
})(ScriptKind = exports.ScriptKind || (exports.ScriptKind = {}));
