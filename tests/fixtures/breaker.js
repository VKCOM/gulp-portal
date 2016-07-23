const { warp } = require('../../lib/portal');
const through = require('through2');

warp(sinkIn => sinkIn.pipe(through.obj((fl, tp, cb) => {
  cb(new Error('I\'m a breaker'));
})));
