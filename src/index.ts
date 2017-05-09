import { WriteOptions } from './index';
import * as parse from 'csv-parse';
import * as stringify from 'csv-stringify';
import { createReadStream, writeFile, createWriteStream } from 'fs';
import { extname } from 'path';
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
    const delimiter = readOptions.delimiter || this.getDefaultDelimiter(src);
    const parseOptions = this.getParseOptions(delimiter);
    createReadStream(src).pipe(parse(parseOptions, cb));
  }

  static createReadStream(src: string, readOptions: ReadOptions = {}): NodeJS.ReadableStream {
    const delimiter = readOptions.delimiter || this.getDefaultDelimiter(src);
    const parseOptions = this.getParseOptions(delimiter);
    return createReadStream(src).pipe(parse(parseOptions));
  }

  static parse(content: string, delimiter: string, cb: ResultCb<any[]>) {
    const parseOptions = this.getParseOptions(delimiter);
    parse(content, parseOptions, cb);
  }

  static write(dest: string, data: any[], cb: ErrorCb): void;
  static write(dest: string, data: any[], opts: WriteOptions, cb: ErrorCb): void;
  static write(dest: string, data: any[], arg1: any, arg2?: any): void {
    const writeOptions = arg2 ? arg1 : {};
    const cb = arg2 ? arg2 : arg1;
    if (!writeOptions.delimiter) writeOptions.delimiter = this.getDefaultDelimiter(dest);
    const stringifyOptions = this.getStringifyOptions(writeOptions, data);
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
    if (!opts.delimiter) opts.delimiter = this.getDefaultDelimiter(dest);
    const stringifyOptions = this.getStringifyOptions(opts);
    const stringifyTransform = stringify(stringifyOptions);
    stringifyTransform.pipe(createWriteStream(dest));
    return stringifyTransform;
  }

  static stringify(data: any[], opts: WriteOptions, cb: ResultCb<string>) {
    if (!opts.delimiter) opts.delimiter = ',';
    const stringifyOptions = this.getStringifyOptions(opts);
    const nestedArrayData = this.getNestedArrayData(data, stringifyOptions.columns);
    stringify(nestedArrayData, stringifyOptions, cb);
  }

  private static getParseOptions(delimiter: string): ParseOptions {
    return {
      delimiter,
      columns: true,
      auto_parse: false,
      skip_empty_lines: true,
      trim: true,
      relax: true
    };
  }

  private static getDefaultDelimiter(filePath: string) {
    return (extname(filePath) === '.tsv') ? '\t' : ',';
  }

  private static getStringifyOptions(writeOptions: WriteOptions, data?: any[]): StringifyOptions {
    const header = true;
    let columns: string[];

    if (writeOptions.columns) {
      columns = writeOptions.columns;
    } else if (data) {
      columns = this.getDataFields(data);
    } else {
      columns = undefined;
    }

    return { delimiter: writeOptions.delimiter, header, columns };
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
  write(chunk: string[] | Object): boolean;
}
