const array = require('stream-array');
const File = require('vinyl');

function testStream(...args) {
  let i = 0;

  function create(contents) {
    return new File({
      cwd: '/home/wonderland/',
      base: '/home/wonderland/test',
      path: `/home/wonderland/test/file${(i++).toString()}.js`,
      contents: new Buffer.from(contents),
    });
  }

  return array(args.map(create));
}

module.exports = {
  testStream,
};
