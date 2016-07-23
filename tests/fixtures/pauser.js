const { warp } = require('../../lib/portal');
const through = require('through2');

warp(sinkIn => sinkIn.pipe(through.obj((fl, tp, cb) => {
  setTimeout(() => cb(null, fl), 500);
})));
