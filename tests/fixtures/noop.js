const through = require('through2');
const { warp } = require('../../lib/portal');

warp(sinkIn => sinkIn.pipe(through.obj((fl, tp, cb) => {
  cb(null, fl);
})));
