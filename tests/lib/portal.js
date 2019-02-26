const portal = require('../../lib/portal');
const { testStream } = require('../utils');
const assert = require('stream-assert');
const { expect } = require('chai');
const path = require('path');
const through = require('through2');

const noop = path.join(process.cwd(), 'tests/fixtures/noop.js');
const pauser = path.join(process.cwd(), 'tests/fixtures/pauser.js');
const breaker = path.join(process.cwd(), 'tests/fixtures/breaker.js');
const goodBreaker = path.join(process.cwd(), 'tests/fixtures/good_breaker.js');

function compare(str, fl) {
  expect(fl.contents.toString()).to.equal(str);
}

const oneStdConfig = {
  workers: 1,
  cacheWorkers: false,
  maxParallel: 1,
  worker: noop,
};

const twoStdConfig = {
  workers: 2,
  cacheWorkers: false,
  maxParallel: 2,
  worker: pauser,
};

function spy() {
  return through.obj((fl, tp, cb) => {
    console.log(fl.contents.toString());
    cb(null, fl);
  });
}

function verifyError(msg, done, err) {
  try {
    expect(err.message).to.match(msg);
    done();
  } catch (except) {
    done(except);
  }
}

describe('gulp portal', () => {
  it('passes through one file through one stream', (done) => {
    testStream('test')
      .pipe(portal.start(oneStdConfig))
      .pipe(assert.length(1))
      .pipe(assert.first(compare.bind(null, 'test')))
      .pipe(assert.end(done));
  });

  it('passes through two file through one stream', (done) => {
    testStream('test', 'test2')
      .pipe(portal.start(oneStdConfig))
      .pipe(assert.length(2))
      .pipe(assert.first(compare.bind(null, 'test')))
      .pipe(assert.second(compare.bind(null, 'test2')))
      .pipe(assert.end(done));
  });

  it('passes through two file through two streams', (done) => {
    const start = Date.now();
    testStream('test', 'test2')
      .pipe(portal.start(twoStdConfig))
      .pipe(assert.length(2))
      .pipe(assert.any(compare.bind(null, 'test')))
      .pipe(assert.any(compare.bind(null, 'test2')))
      .pipe(assert.end((err) => {
        const estimated = Date.now() - start;
        expect(estimated).to.be.within(500, 1000);
        done(err);
      }));
  });

  it('passes through two file through two streams with 1 concurrency', (done) => {
    const start = Date.now();
    const curCfg = Object.assign({}, twoStdConfig, {
      maxParallel: 1,
    });
    testStream('test', 'test2')
      .pipe(portal.start(curCfg))
      .pipe(assert.length(2))
      .pipe(assert.any(compare.bind(null, 'test')))
      .pipe(assert.any(compare.bind(null, 'test2')))
      .pipe(assert.end(() => {
        const estimated = Date.now() - start;
        expect(estimated).to.be.above(1000);
        done();
      }));
  });

  it('passes through one file through one stream and gets error, but can\'t route back', (done) => {
    testStream('test')
      .pipe(portal.start(Object.assign({}, oneStdConfig, { worker: breaker, timeout: 500 })))
      .on('error', verifyError.bind(null, /didn't return from portal/, done));
  });

  it('passes through one file through one stream and gets error', (done) => {
    testStream('test')
      .pipe(portal.start(Object.assign({}, oneStdConfig, { worker: goodBreaker, timeout: 500 })))
      .on('error', verifyError.bind(null, /I'm a breaker/, done));
  });
});
