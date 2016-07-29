import * as parse from 'csv-parse';
import * as stringify from 'csv-stringify';
import {createReadStream, writeFile, createWriteStream} from 'fs';
import {extname} from 'path';
import * as _ from 'lodash';

export class CSVTranslator {
  read(src: string, cb: ResultCb<any[]>): any;
  read(src: string, opts: ReadOptions, cb: ResultCb<any[]>): void;
  read(src: string, arg1: any, arg2?: any) {
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

  createReadStream(src: string, readOptions: ReadOptions = {}): NodeJS.ReadWriteStream {
    const parseOptions = this.getParseOptions(src, readOptions);
    return createReadStream(src).pipe(parse(parseOptions));
  }

  private getParseOptions(src: string, readOptions: ReadOptions): ParseOptions {
    return {
      delimiter: readOptions.delimiter || this.getDefaultDelimiter(src),
      columns: true,
      auto_parse: true,
      skip_empty_lines: true,
      trim: true
    };
  }

  private getDefaultDelimiter(filePath: string) {
    return (extname(filePath) === '.tsv') ? '\t' : ',';
  }

  write(dest: string, data: any[], cb: ErrorCb): void;
  write(dest: string, data: any[], opts: WriteOptions, cb: ErrorCb): void;
  write(dest: string, data: any[], arg1: any, arg2?: any): void {
    let writeOptions: WriteOptions;
    let cb: ErrorCb;
    if (arg2 === undefined) {
      writeOptions = {};
      cb = arg1;
    } else {
      writeOptions = arg1;
      cb = arg2;
    }
    const stringifyOptions = this.getStringifyOptions(dest, writeOptions, data);
    const nestedArrayData = this.getNestedArrayData(data, stringifyOptions.columns);
    stringify(nestedArrayData, stringifyOptions, (err, stringData) => {
      if (err) { return cb(err); }
      writeFile(dest, stringData, cb);
    });
  }

  createWriteStream(dest: string, opts: WriteOptions = {}): stringify.Stringifier {
    const stringifyOptions = this.getStringifyOptions(dest, opts);
    const stringifyTransform = stringify(stringifyOptions);
    stringifyTransform.pipe(createWriteStream(dest));
    return stringifyTransform;
  }

  private getStringifyOptions(dest: string, writeOptions: WriteOptions, data?: any[]): StringifyOptions {
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

  private getDataFields(data: any[]): string[] {
    return _(data).flatMap(_.keys).uniq().value() as string[];
  }

  private getNestedArrayData(data: any[], columns: string[]): any[] {
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
