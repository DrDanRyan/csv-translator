import {CSVTranslator as csv} from './index';
import {suite, test} from 'mocha-typescript';
import {expect} from 'chai';
const csvSrc = './src/test-input.csv';
const tsvSrc = './src/test-input.tsv';
const txtSrc = './src/test-input.txt';
const dest = './src/test-output.txt';

@suite class Read {
  @test readCSV(done: Done) {
    csv.read(csvSrc, (err, data) => {
      if (err) { return done(err); }
      expect(data).to.deep.equal([
        {a: '1', b: '2'},
        {a: '3', b: '4'}
      ]);
      done();
    });
  }

  @test readTSV(done: Done) {
    csv.read(tsvSrc, (err, data) => {
      if (err) { return done(err); }
      expect(data).to.deep.equal([
        {a: '1', b: '2'},
        {a: '3', b: '4'}
      ]);
    });
    done();
  }

  @test delimiter(done: Done) {
    csv.read(txtSrc, {delimiter: '\t'}, (err, data) => {
      if (err) { return done(err); }
      expect(data).to.deep.equal([
        {a: '1', b: '2'},
        {a: '3', b: '4'}
      ]);
    });
    done();
  }
}


@suite class CreateReadStream {
  @test readStreamCSV(done: Done) {
    const readable = csv.createReadStream(csvSrc);
    let counter = 0;
    readable.on('data', (obj: any) => {
      if (counter === 0) {
        counter++;
        expect(obj).to.deep.equal({a: '1', b: '2'});
      } else {
        expect(obj).to.deep.equal({a: '3', b: '4'});
      }
    });
    readable.on('finish', done);
  }

  @test readStreamTSV(done: Done) {
    const readable = csv.createReadStream(tsvSrc);
    let counter = 0;
    readable.on('data', (obj: any) => {
      if (counter === 0) {
        counter++;
        expect(obj).to.deep.equal({a: '1', b: '2'});
      } else {
        expect(obj).to.deep.equal({a: '3', b: '4'});
      }
    });
    readable.on('finish', done);
  }

  @test readStreamDelimiter(done: Done) {
    const readable = csv.createReadStream(txtSrc, {delimiter: '\t'});
    let counter = 0;
    readable.on('data', (obj: any) => {
      if (counter === 0) {
        counter++;
        expect(obj).to.deep.equal({a: '1', b: '2'});
      } else {
        expect(obj).to.deep.equal({a: '3', b: '4'});
      }
    });
    readable.on('finish', done);
  }
}


@suite class Write {
  @test writeEvenColumns(done: Done) {
    const data = [{a: 'a', b: 'b'}, {a: 'c', b: 'd'}];
    csv.write(dest, data, err => {
      if (err) { return done(err); }
      csv.read(dest, (err2, newData) => {
        if (err2) { return done(err2); }
        expect(newData).to.deep.equal(data);
        done();
      });
    });
  }

  @test writeUnionColumns(done: Done) {
    const data = [{a: 'a'}, {a: 'c', b: 'd'}];
    csv.write(dest, data, err => {
      if (err) { return done(err); }
      csv.read(dest, (err2, newData) => {
        if (err2) { return done(err2); }
        expect(newData).to.deep.equal([{a: 'a', b: ''}, {a: 'c', b: 'd'}]);
        done();
      });
    });
  }

  @test writeRestrictedColumns(done: Done) {
    const data = [{a: '1', b: '2'}, {a: '3'}];
    csv.write(dest, data, {columns: ['a']}, err => {
      if (err) { return done(err); }
      csv.read(dest, (err2, newData) => {
        if (err2) { return done(err2); }
        expect(newData).to.deep.equal([{a: '1'}, {a: '3'}]);
        done();
      });
    });
  }
}

@suite class CreateWriteStream {
  @test writeStreamEvenColumns(done: Done) {
    const data = [{a: 'a', b: 'b'}, {a: 'c', b: 'd'}];
    const writable = csv.createWriteStream(dest).on('finish', () => {
      csv.read(dest, (err, newData) => {
        expect(newData).to.deep.equal(data);
        done();
      });
    }).on('error', done);
    writable.write(data[0]);
    writable.write(data[1]);
    writable.end();
  }

  @test writeStreamDiscoverColumns(done: Done) {
    const data = [{a: '1', b: '2'}, {a: '3', c: '4'}];
    const writable = csv.createWriteStream(dest).on('finish', () => {
      csv.read(dest, (err, newData) => {
        expect(newData).to.deep.equal([{a: '1', b: '2'}, {a: '3', b: ''}]);
        done();
      });
    }).on('error', done);
    writable.write(data[0]);
    writable.write(data[1]);
    writable.end();
  }

  @test writeStreamSpecifyColumns(done: Done) {
    const data = [{a: '1', b: '2'}, {a: '3', c: '4'}];
    const writable = csv.createWriteStream(dest, {columns: ['a', 'c']}).on('finish', () => {
      csv.read(dest, (err, newData) => {
        expect(newData).to.deep.equal([{a: '1', c: ''}, {a: '3', c: '4'}]);
        done();
      });
    }).on('error', done);
    writable.write(data[0]);
    writable.write(data[1]);
    writable.end();
  }
}


interface Done {
  (err?: Error): void;
}
