var cli = require('./../index');
var bench = {};

bench.get = function (times) {
  var ccount = 0;
  cli.initClient('group_ju', [
    {host: '10.235.144.116', port: 5198}
  ], function (err) {
    if (err) {
      console.log(err);
    }
    console.time(times + ' times get');
    for (var i = 0; i < times; i++) {
      cli.get('hello', function (err, data) {
        ccount++;
        if (err) {
          console.error(err);
        }
        if (ccount === times) {
          console.timeEnd(times + ' times get');
          process.exit(0);
        }
      });
    }
  });
}

bench.set = function (times) {
  var ccount = 0;
  cli.initClient('group_ju', [
    {host: '10.235.144.116', port: 5198}
  ], function (err) {
    if (err) {
      console.log(err);
    }
    console.time(times + ' times set');
    for (var i = 0; i < times; i++) {
      cli.set('hello2test', 'lzstat_uv=9515746301159965720|2592155@2837774@2738597@12' +
        '16880@2786630@2775310@1791451@2043323@1723936@2592119@1296239@2225939@288' +
        '1505@2857556@2059594@2208862@2581747@2692646@2751004@2490638@2852799@2735862@273' +
        '5864@2229691@2558276@2800965@2581759@2836758@2920265@2910057@2285988', function (err, succ) {
        ccount++;
        if (err || !succ) {
          console.error(err);
        }
        if (ccount === times) {
          console.timeEnd(times + ' times set');
          process.exit(0);
        }
      });
    }
  });
}

function main(argv) {
  var test = argv[2];
  var times = argv[3] || 1000;
  if (!bench[test]) {
    console.log('usage: node benchmark.js benchname [times=1000]');
    return process.exit(0);
  }
  bench[test](times);
}

main(process.argv);