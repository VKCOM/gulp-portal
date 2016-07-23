const portal = require('../../lib/portal');
const { testStream } = require('../utils');
const assert = require('stream-assert');
const { expect } = require('chai');
const path = require('path');

const noop = path.join(process.cwd(), 'tests/fixtures/noop.js');

describe('gulp portal', () => {
  it('passes through one file', (done) => {
    testStream('test')
      .pipe(portal.start({
        workers: 1,
        cacheWorkers: false,
        maxListeners: 200,
        worker: noop,
      })).pipe(assert.length(1))
      .pipe(assert.first(fl => expect(fl.contents.toString()).to.equal('test')))
      .pipe(assert.end(done));
  });
});
