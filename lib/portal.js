const assign = require('object-assign');
const cluster = require('cluster');
const through = require('through2');
const throughc = require('through2-concurrent');
const uuid = require('node-uuid');
const File = require('vinyl');
const plumber = require('gulp-plumber');
const gutils = require('gulp-util');

const CLUSTER_CACHE = {};

function startWorker(workerScript, workers, maxListener) {
  const workCluster = {
    workers: [],
  };

  if (cluster.isMaster) {
    cluster.setupMaster({
      exec: workerScript,
    });


    for (let i = 0; i < workers; i++) {
      const worker = cluster.fork();
      workCluster.workers.push(worker.id);
    }

    workCluster.workers.forEach(clusterId => {
      cluster.workers[clusterId].setMaxListeners(maxListener);
    });
  }

  return workCluster;
}

function getWorkCluster(opts) {
  let workCluster;
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

  const serializable = file.clone();
  serializable._contents = serializable.contents.toString();

  return JSON.stringify({ type: 'file', file: serializable });
}

function deserialize(message) {
  const parsed = JSON.parse(message);
  if (parsed.type === 'error') {
    return parsed;
  }

  const file = new File({});

  Object.keys(parsed.file).forEach(key => {
    file[key] = parsed.file[key];
  });

  file._contents = new Buffer(file._contents);

  const res = { type: 'file', file };
  return res;
}

function findFuid(fuids, name) {
  return Object.keys(fuids)
    .filter((uid) => fuids[uid] === name)[0];
}

function warp(fn) {
  const FuidCache = {};

  const sinkIn = through.obj((file, tp, cb) => { cb(null, file); });

  const sinkinPiped = sinkIn.pipe(plumber({
    errorHandler: (err) => {
      if (err.fileName) {
        const fuid = findFuid(FuidCache, err.fileName);
        FuidCache[fuid] = undefined;
        process.send(fuid + serialize({ type: 'error', error: err }));
      } else {
        console.log('Can\'t route this error back, no fileName option');
      }
    },
  }));

  const sinkOut = through.obj((file, tp, cb) => {
    const fuid = findFuid(FuidCache, file.path);
    FuidCache[fuid] = undefined;
    process.send(fuid + serialize(file));
    cb();
  });

  process.on('message', (message) => {
    const fuid = message.slice(0, 36);
    const cnt = message.slice(36);
    const file = deserialize(cnt).file;

    FuidCache[fuid] = file.path;
    sinkIn.push(file);
  });

  fn(sinkinPiped).pipe(sinkOut);
}

function listenWorker(fuid, resolve, worker, message) {
  const rfuid = message.slice(0, fuid.length);

  if (fuid !== rfuid) {
    worker.once('message', listenWorker.bind(null, fuid, resolve, worker));
    return;
  }

  const result = deserialize(message.slice(fuid.length));
  resolve(result);
}

function sendToWorker(workerId, message, timeout, name) {
  const fuid = uuid.v4();
  let ctimeout = timeout;
  return new Promise((resolve) => {
    const worker = cluster.workers[workerId];
    worker.send(fuid + message);
    worker.once('message', listenWorker.bind(null, fuid, resolve, worker));

    ctimeout = setTimeout(() => {
      resolve({
        type: 'error',
        error: new gutils.PluginError('gulp-portal',
          new Error(`File ${name} didn\'t return from portal`), {
            showStack: false,
            fileName: name,
          }),
      });
    }, ctimeout);
  }).then((result) => {
    clearTimeout(ctimeout);
    return result;
  });
}

function closeCluster(workCluster) {
  workCluster.workers.forEach((clusterId) => {
    cluster.workers[clusterId].kill();
  });
}

function start(copts) {
  const opts = assign({
    workers: 1,
    cacheWorkers: false,
    worker: null,
    maxListeners: 50,
    timeout: 10000,
    maxParallel: 20,
  }, copts);

  if (!opts.worker) {
    throw new Error('You must specify worker for gulp-portal!');
  }

  const workCluster = getWorkCluster(opts);

  let roundCounter = 0;
  const jobs = [];

  const stream = throughc({
    objectMode: true,
    maxConcurrency: opts.maxParallel,
  }, function write(file, type, callback) {
    const workerId = workCluster.workers[roundCounter % workCluster.workers.length];
    if (opts.debug) {
      console.time(file.relative);
    }

    jobs.push(sendToWorker(workerId, serialize(file), opts.timeout, file.path)
      .then((result) => {
        if (opts.debug) {
          console.timeEnd(file.relative);
        }

        if (result.type === 'file') {
          callback(null, result.file);
        } else {
          callback(new gutils.PluginError('gulp-portal', result.error));
        }
      }).catch((err) => {
        callback(err);
      }));

    roundCounter++;
  }, function end() {
    Promise.all(jobs).then(() => {
      if (!opts.cacheWorkers) {
        closeCluster(workCluster);
      }

      this.push(null);
    });
  });

  return stream;
}

module.exports = {
  warp,
  start,
};
