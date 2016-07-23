const { warp } = require('../../lib/portal');
const through = require('through2');
const gutils = require('gulp-util');

warp(sinkIn => sinkIn.pipe(through.obj((fl, tp, cb) => {
  const e = new Error('I\'m a breaker');
  cb(new gutils.PluginError('good-breaker', e, {
    fileName: fl.path,
    showStack: true,
  }));
})));
