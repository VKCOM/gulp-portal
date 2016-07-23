'use strict';

var assign = require('object-assign');
var cluster = require('cluster');
var through = require('through2');
var throughc = require('through2-concurrent');
var uuid = require('node-uuid');
var File = require('vinyl');
var plumber = require('gulp-plumber');
var gutils = require('gulp-util');

var CLUSTER_CACHE = {};

function startWorker(workerScript, workers, maxListener) {
  var workCluster = {
    workers: []
  };

  if (cluster.isMaster) {
    cluster.setupMaster({
      exec: workerScript
    });

    for (var i = 0; i < workers; i++) {
      var worker = cluster.fork();
      workCluster.workers.push(worker.id);
    }

    workCluster.workers.forEach(function (clusterId) {
      cluster.workers[clusterId].setMaxListeners(maxListener);
    });
  }

  return workCluster;
}

function getWorkCluster(opts) {
  var workCluster = void 0;
  if (opts.cacheWorkers && CLUSTER_CACHE[opts.worker]) {
    workCluster = CLUSTER_CACHE[opts.worker];
  } else {
    workCluster = startWorker(opts.worker, opts.workers, opts.maxListeners);

    if (opts.cacheWorkers) {
      CLUSTER_CACHE[opts.worker] = workCluster;
    }
  }

  return workCluster;
}

function serialize(file) {
  if (file.type === 'error') {
    return JSON.stringify(file);
  }

  var serializable = file.clone();
  serializable._contents = serializable.contents.toString();

  return JSON.stringify({ type: 'file', file: serializable });
}

function deserialize(message) {
  var parsed = JSON.parse(message);
  if (parsed.type === 'error') {
    return parsed;
  }

  var file = new File({});

  Object.keys(parsed.file).forEach(function (key) {
    file[key] = parsed.file[key];
  });

  file._contents = new Buffer(file._contents);

  var res = { type: 'file', file: file };
  return res;
}

function findFuid(fuids, name) {
  return Object.keys(fuids).filter(function (uid) {
    return fuids[uid] === name;
  })[0];
}

function warp(fn) {
  var FuidCache = {};

  var sinkIn = through.obj(function (file, tp, cb) {
    cb(null, file);
  });

  var sinkinPiped = sinkIn.pipe(plumber({
    errorHandler: function errorHandler(err) {
      if (err.fileName) {
        var fuid = findFuid(FuidCache, err.fileName);
        FuidCache[fuid] = undefined;
        process.send(fuid + serialize({ type: 'error', error: err }));
      } else {
        console.log('Can\'t route this error back, no fileName option');
      }
    }
  }));

  var sinkOut = through.obj(function (file, tp, cb) {
    var fuid = findFuid(FuidCache, file.path);
    FuidCache[fuid] = undefined;
    process.send(fuid + serialize(file));
    cb();
  });

  process.on('message', function (message) {
    var fuid = message.slice(0, 36);
    var cnt = message.slice(36);
    var file = deserialize(cnt).file;

    FuidCache[fuid] = file.path;
    sinkIn.push(file);
  });

  fn(sinkinPiped).pipe(sinkOut);
}

function listenWorker(fuid, resolve, worker, message) {
  var rfuid = message.slice(0, fuid.length);

  if (fuid !== rfuid) {
    worker.once('message', listenWorker.bind(null, fuid, resolve, worker));
    return;
  }

  var result = deserialize(message.slice(fuid.length));
  resolve(result);
}

function sendToWorker(workerId, message, timeout, name) {
  var fuid = uuid.v4();
  var timeoutId = void 0;
  return new Promise(function (resolve) {
    var worker = cluster.workers[workerId];
    worker.send(fuid + message);
    worker.once('message', listenWorker.bind(null, fuid, resolve, worker));

    timeoutId = setTimeout(function () {
      resolve({
        type: 'error',
        error: new gutils.PluginError('gulp-portal', new Error('File ' + name + ' didn\'t return from portal'), {
          showStack: false,
          fileName: name
        })
      });
    }, timeout);
  }).then(function (result) {
    clearTimeout(timeoutId);
    return result;
  });
}

function closeCluster(workCluster) {
  workCluster.workers.forEach(function (clusterId) {
    cluster.workers[clusterId].kill();
  });
}

function start(copts) {
  var opts = assign({
    workers: 1,
    cacheWorkers: false,
    worker: null,
    maxListeners: 50,
    timeout: 10000,
    maxParallel: 20
  }, copts);

  if (!opts.worker) {
    throw new Error('You must specify worker for gulp-portal!');
  }

  var workCluster = getWorkCluster(opts);

  var roundCounter = 0;
  var jobs = [];

  var stream = throughc({
    objectMode: true,
    maxConcurrency: opts.maxParallel
  }, function write(file, type, callback) {
    var workerId = workCluster.workers[roundCounter % workCluster.workers.length];
    if (opts.debug) {
      console.time(file.relative);
    }

    jobs.push(sendToWorker(workerId, serialize(file), opts.timeout, file.path).then(function (result) {
      if (opts.debug) {
        console.timeEnd(file.relative);
      }

      if (result.type === 'file') {
        callback(null, result.file);
      } else {
        callback(new gutils.PluginError('gulp-portal', result.error));
      }
    }).catch(function (err) {
      callback(err);
    }));

    roundCounter++;
  }, function end() {
    var _this = this;

    Promise.all(jobs).then(function () {
      if (!opts.cacheWorkers) {
        closeCluster(workCluster);
      }

      _this.push(null);
    });
  });

  return stream;
}

module.exports = {
  warp: warp,
  start: start
};