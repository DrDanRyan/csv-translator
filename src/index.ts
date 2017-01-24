import * as parse from 'csv-parse';
import * as stringify from 'csv-stringify';
import {createReadStream, writeFile, createWriteStream} from 'fs';
import {extname} from 'path';
import * as _ from 'lodash';
import { parallel } from 'async';

export class CSVTranslator {
  static read(src: string, cb: ResultCb<any[]>): any;
  static read(src: string, opts: ReadOptions, cb: ResultCb<any[]>): void;
  static read(src: string, arg1: any, arg2?: any) {
    let readOptions: ReadOptions;
    let cb: ResultCb<any[]>;
    if (arg2 === undefined) {
      readOptions = {};
      cb = arg1;
    } else {
      readOptions = arg1;
      cb = arg2;
    }
    const parseOptions = this.getParseOptions(src, readOptions);
    createReadStream(src).pipe(parse(parseOptions, cb));
  }

  static createReadStream(src: string, readOptions: ReadOptions = {}): NodeJS.ReadableStream {
    const parseOptions = this.getParseOptions(src, readOptions);
    return createReadStream(src).pipe(parse(parseOptions));
  }

  static write(dest: string, data: any[], cb: ErrorCb): void;
  static write(dest: string, data: any[], opts: WriteOptions, cb: ErrorCb): void;
  static write(dest: string, data: any[], arg1: any, arg2?: any): void {
    const writeOptions = arg2 ? arg1 : {};
    const cb = arg2 ? arg2 : arg1;
    const stringifyOptions = this.getStringifyOptions(dest, writeOptions, data);
    const nestedArrayData = this.getNestedArrayData(data, stringifyOptions.columns);
    stringify(nestedArrayData, stringifyOptions, (err, stringData) => {
      if (err) { return cb(err); }
      writeFile(dest, stringData, cb);
    });
  }

  static writeParallel(pairs: [string, any[]][], mainCb: ErrorCb): void;
  static writeParallel(pairs: [string, any[]][], opts: WriteOptions, mainCb: ErrorCb): void;
  static writeParallel(pairs: [string, any[]][], arg1: any, arg2?: any): void {
    const writeOptions = arg2 ? arg1 : {};
    const mainCb = arg2 ? arg2 : arg1;
    const tasks = pairs.map(pair => {
      const [filename, data] = pair;
      return (cb: ErrorCb) => {
        if (data.length === 0) { return cb(null); }
        this.write(filename, data, writeOptions, cb);
      };
    });
    parallel(tasks, (err: Error) => mainCb(err));
  }

  static createWriteStream(dest: string, opts: WriteOptions = {}): CSVWriteStream {
    const stringifyOptions = this.getStringifyOptions(dest, opts);
    const stringifyTransform = stringify(stringifyOptions);
    stringifyTransform.pipe(createWriteStream(dest));
    return stringifyTransform;
  }

  private static getParseOptions(src: string, readOptions: ReadOptions): ParseOptions {
    return {
      delimiter: readOptions.delimiter || this.getDefaultDelimiter(src),
      columns: true,
      auto_parse: true,
      skip_empty_lines: true,
      trim: true,
      relax: true
    };
  }

  private static getDefaultDelimiter(filePath: string) {
    return (extname(filePath) === '.tsv') ? '\t' : ',';
  }

  private static getStringifyOptions(dest: string, writeOptions: WriteOptions, data?: any[]): StringifyOptions {
    const delimiter = writeOptions.delimiter || this.getDefaultDelimiter(dest);
    const header = true;
    let columns: string[];

    if (writeOptions.columns) {
      columns = writeOptions.columns;
    } else if (data) {
      columns = this.getDataFields(data);
    } else {
      columns = undefined;
    }

    return {delimiter, header, columns};
  }

  private static getDataFields(data: any[]): string[] {
    return _(data).flatMap(_.keys).uniq().value() as string[];
  }

  private static getNestedArrayData(data: any[], columns: string[]): any[] {
    return _.map(data, obj => {
      return _.map(columns, col => {
        return obj[col];
      });
    });
  }
}


export interface ReadOptions {
  delimiter?: string;
}

export interface WriteOptions {
  delimiter?: string;
  columns?: string[];
}

interface ParseOptions {
  delimiter: string;
  columns: boolean;
  auto_parse: boolean;
  skip_empty_lines: boolean;
  trim: boolean;
  relax: boolean;
}

interface StringifyOptions {
  delimiter: string;
  columns?: string[];
  header: boolean;
}

export interface ErrorCb {
  (err?: Error): void;
}

export interface ResultCb<T> {
  (err: Error, result?: T): void;
}

export interface CSVWriteStream extends NodeJS.WritableStream {
  write(chunk: string[]|Object): boolean;
}
