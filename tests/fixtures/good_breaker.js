const through = require('through2');
const PluginError = require('plugin-error');
const { warp } = require('../../lib/portal');

warp(sinkIn => sinkIn.pipe(through.obj((fl, tp, cb) => {
  const e = new Error('I\'m a breaker');
  cb(new PluginError('good-breaker', e, {
    fileName: fl.path,
    showStack: true,
  }));
})));
