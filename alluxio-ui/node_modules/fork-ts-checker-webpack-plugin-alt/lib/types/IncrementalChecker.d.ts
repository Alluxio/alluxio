import * as ts from 'typescript';
import { Configuration, Linter } from 'tslint';
import { FilesRegister } from './FilesRegister';
import { FilesWatcher } from './FilesWatcher';
import { NormalizedMessage } from './NormalizedMessage';
import { CancellationToken } from './CancellationToken';
import * as minimatch from 'minimatch';
import { TypeScriptInstance } from './TypeScriptInstance';
interface ConfigurationFile extends Configuration.IConfigurationFile {
    linterOptions?: {
        typeCheck?: boolean;
        exclude?: string[];
    };
}
export declare class IncrementalChecker {
    programConfigFile: string;
    compilerOptions: object;
    linterConfigFile: string | false;
    watchPaths: string[];
    workNumber: number;
    workDivision: number;
    checkSyntacticErrors: boolean;
    files: FilesRegister;
    linter: Linter;
    linterConfig: ConfigurationFile;
    linterExclusions: minimatch.IMinimatch[];
    typescript: TypeScriptInstance;
    program: ts.Program;
    programConfig: ts.ParsedCommandLine;
    watcher: FilesWatcher;
    vue: boolean;
    constructor(typescriptPath: string, programConfigFile: string, compilerOptions: object, linterConfigFile: string | false, watchPaths: string[], workNumber: number, workDivision: number, checkSyntacticErrors: boolean, vue: boolean);
    static loadProgramConfig(typescript: TypeScriptInstance, configFile: string, compilerOptions: object): ts.ParsedCommandLine;
    static loadLinterConfig(configFile: string): ConfigurationFile;
    static createProgram(typescript: TypeScriptInstance, programConfig: ts.ParsedCommandLine, files: FilesRegister, watcher: FilesWatcher, oldProgram: ts.Program): ts.Program;
    static createLinter(program: ts.Program): any;
    static isFileExcluded(filePath: string, linterExclusions: minimatch.IMinimatch[]): boolean;
    nextIteration(): void;
    loadVueProgram(): ts.Program;
    loadDefaultProgram(): ts.Program;
    hasLinter(): boolean;
    getDiagnostics(cancellationToken: CancellationToken): NormalizedMessage[];
    getLints(cancellationToken: CancellationToken): NormalizedMessage[];
}
export {};
