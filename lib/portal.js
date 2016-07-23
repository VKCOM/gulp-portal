var assign = require('object-assign');;
var cluster = require('cluster');
var through = require('through2');
var uuid = require('node-uuid');
var vinyl = require('vinyl');
var plumber = require('gulp-plumber');
var gutils = require('gulp-util');

var clusterCache = {};

function startWorker(workerScript, workers, maxListener) {
  var workCluster = {
    workers: []
  };

  if(cluster.isMaster) {
    cluster.setupMaster({
      exec: workerScript
    });

    var clustersAlready = Object.keys(cluster.workers).length;

    for (var i = clustersAlready + 1; i <= clustersAlready + workers; i++) {
      cluster.fork();
      workCluster.workers.push(i);
    }

    workCluster.workers.forEach(function(clusterId) {
      cluster.workers[clusterId].setMaxListeners(maxListener);
    });
  }

  return workCluster;
}

function getWorkCluster(opts) {
  var workCluster;
  if (opts.cacheWorkers && clusterCache[opts.worker]) {
    workCluster = clusterCache[opts.worker];
  } else {

    workCluster = startWorker(opts.worker, opts.workers, opts.maxListeners)

    if (opts.cacheWorkers) {
      clusterCache[opts.worker] = workCluster;
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

  var file = new vinyl({});

  for (var key in parsed.file) {
    file[key] = parsed.file[key];
  }

  file._contents = new Buffer(file._contents);

  var res = { type: 'file', file: file };
  return res;
}

function findFuid(fuids, name) {
  return Object.keys(fuids)
    .filter(function(uid) {
      return fuids[uid] === name;
    })[0];
}

function warp(fn) {
  var FuidCache = {};

  var sinkIn = through.obj(function(file, tp, cb) { cb(null, file) });

  var sinkinPiped = sinkIn.pipe(plumber({
    errorHandler: function(err) {
      if (err.fileName) {
        var fuid = findFuid(FuidCache, err.fileName);
        FuidCache[fuid] = undefined;
        process.send(fuid + serialize({ type: 'error',  error: err}));
      } else {
        console.log('Can\'t route this error back, no fileName option');
      }
    }
  }));

  var sinkOut = through.obj(function(file, tp, cb) {
    var fuid = findFuid(FuidCache, file.path);
    FuidCache[fuid] = undefined;
    process.send(fuid + serialize(file));
    cb();
  });

  process.on('message', function(message) {
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
  var timeout;
  return new Promise(function(resolve) {
    var worker = cluster.workers[workerId];
    worker.send(fuid + message);
    worker.once('message', listenWorker.bind(null, fuid, resolve, worker));

    timeout = setTimeout(function() {
      resolve({
        type: 'error',
        error: new gutils.PluginError('gulp-portal', new Error('File ' + name + ' didn\'t return from portal'), {
          showStack: false,
          fileName: name
        })
      })
    }, timeout);
  }).then(function(result) {
    clearTimeout(timeout);
    return result;
  });
}

function closeCluster(workCluster) {
  workCluster.workers.forEach(function(clusterId) {
    cluster.workers[clusterId].kill();
  });
}

function start(opts) {
  opts = assign({
    workers: 1,
    cacheWorkers: false,
    worker: null,
    maxListeners: 50,
    timeout: 10000,
    maxParallel: 20
  }, opts);

  if (!opts.worker) {
    throw new Error('You must specify worker for gulp-portal!');
  }

  var workCluster = getWorkCluster(opts);

  var roundCounter = 0;
  var jobs = [];

  var currentJobs = 0;
  var currentWaitCb = false;

  var stream = through({ objectMode: true, highWaterMark: 500 }, function(file, type, cb) {
    var workerId = workCluster.workers[roundCounter % workCluster.workers.length];
    currentJobs++;
    currentWaitCb = cb;
    console.time(file.relative);
    jobs.push(sendToWorker(workerId, serialize(file), opts.timeout, file.path)
      .then(function(result) {
        currentJobs--
        console.timeEnd(file.relative);
        if (result.type === 'file') {
          this.push(result.file);
        } else {
          setImmediate(function() {
            console.log(result);
            this.emit('error', new gutils.PluginError('gulp-portal', result.error));
          }.bind(this));
        }
        if (currentWaitCb) {
          var callCb = currentWaitCb;
          currentWaitCb = false;
          callCb();
        }
      }.bind(this)).catch(function(err) {
        console.log(err);
      }));

    if (currentJobs <= opts.maxParallel) {
      cb();
      currentWaitCb = false;
    }
    roundCounter++;
  }, function() {
    Promise.all(jobs).then(function() {
      if (!opts.cacheWorkers) {
        closeCluster(workCluster);
      }

      this.push(null);
    }.bind(this))
  });

  return stream;

}

module.exports = {
  warp: warp,
  start: start
};
