
/*!
 * tair - lib/client.js
 * Copyright(c) 2012 Taobao.com
 * Author: kate.sf <kate.sf@taobao.com>
 */

var configServer = require('./configServer');
var comm = require('./comm');
var packet = require('./packet');
var consts = require('./const');
var utils = require('./utils');

/**
 * initial clients. must be first called
 * @type {Function}
 * @params groupnName：group name of tair
 * @params hostList: config server list of tair, like [{host: '10.235.144.116', port: 5198}]
 * @params options:
 *        - heartBeatInterval = 10 * 1000 {Number} interval time for heartbeat, mili-seconds
 *        - timeout = 5000 {Number}, timeout for network, mili-seconds
 * @params callback(err):
 */
var Tair = module.exports = function (groupName, hostList, options, callback) {

  if (!(this instanceof Tair)) {
    return new Tair(groupName, hostList, options, callback);
  }
  options = options || {};
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }

  callback = callback || function () {
  };
  if (!groupName || !hostList || !callback) {
    return;
  }
  this.heartBeatInterval = options.heartBeatInterval || 10 * 1000;
  this.setTimeout(options.timeout);

  var that = this;
  this.requestQueue = [];
  // init the serverlist etc. only need to init once once program run.
  this.bucketCount = 0;
  this.copyCount = 0;
  this.aliveNodes = [];
  this.serverList = [];
  this.configVersion = 0;

  this.retrieveConfigure(groupName, hostList, function (err, res) {
    if (err) {
      return callback(err);
    }
    callback(null);
    if (res && res.serverList) {
      // run heart beat once to make connect to all datanodes
      that.heartbeat();
      // heartbeat interval
      if (!that._hInterval) {
        that._hInterval = setInterval(that.heartbeat.bind(that), that.heartBeatInterval);
      }
    }
    if (that.requestQueue && that.requestQueue.length > 0) {
      for (var i = 0; i < that.requestQueue.length; i++) {
        var method = that.requestQueue[i].method;
        that[method].apply(that.requestQueue[i].that, that.requestQueue[i].args);
      }
      delete this.requestQueue;
    }
    setInterval(function () {
      var now = new Date().getTime();
      if (that.inited && (now - that.lastSyncConfig > 120 * 1000)) {
        that.retrieveConfigure(that._groupName, that._hostList, function () {
        });
      }
    }, 60 * 1000);
  })
};
Tair.prototype.heartbeat = function (callback) {
  callback = callback || function () {
  };
  // get a key each datanode, for datanode does not eval key`s hash is right when get, one key is enouge.
  var _packet = packet.requestGetOrRemovePacket(0, 'heartbeatkey_nodejs');
  var _servers = this.serverList[0];
  var serverMap = {};
  for (var i = 0; i < _servers.length; i++) {
    var ip = _servers[i].host >>> 0;
    var addr = {host: utils.longToIP(ip), port: _servers[i].port, success: (ip !== 0)};
    // there`s buklet info in serverlist, so we should build a map to ensure one datenode one request
    serverMap[ip] = addr;
  }
  var finished = 0;
  var total = 0;
  var _int = this.heartBeatInterval;
  var that = this;
  for (var key in serverMap) {
    total++;
    comm.getData(serverMap[key], _packet, function (err, data) {
      if (err) {
        console.error("HeartBeat Error! Error: %s", err.toString());
      }
      finished++;
      if (finished >= total) {
        that.heartBeatCount = (that.heartBeatCount || 0) + 1;
        callback(finished);
      }
    });
  }

};
Tair.prototype.setTimeout = function (time) {
  comm.globalTimeout = time || 5000;
};

Tair.prototype.retrieveConfigure = function (groupName, hostList, callback) {
  var that = this;
  configServer.retrieveConfigure.apply(this, [groupName, 0, hostList, function (err, res) {
    if (err) {
      return callback(err);
    }
    that._groupName = groupName;
    that._hostList = hostList;
    that.inited = true;
    that.lastSyncConfig = new Date().getTime();
    callback(null, res);

  }]);
}

Tair.prototype.getDataNode = function () {
  return configServer.getDataNode.apply(this, Array.prototype.slice.apply(arguments));
}

/**
 * set / setEx method
 * @type {Function}
 * @params key：must be string, the key to set
 * @params value: usually string, the value to set
 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
 * @params callback(err, success):
 */
Tair.prototype.set = Tair.prototype.setEx = function (key, value, expire, namespace, version, callback) {
  var _expire = typeof expire === 'number' ? expire : 0;
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  var _version = typeof version === 'number' ? version : 0;
  if (typeof expire === 'function') {
    callback = expire;
  }
  if (typeof namespace === 'function') {
    callback = namespace;
  }
  if (typeof version === 'function') {
    callback = version;
  }
  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }
  if (value === null) {
    return callback(new Error('Tair: params lack.'), false);
  }
  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'set', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }
  var _packet = packet.requestPutPacket(_namespace, _version, key, value, _expire);
  var _addr = this.getDataNode(key);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), false);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, false);
  }
  if (typeof value !== 'number' && value.length >= consts.TAIR_MALLOC_MAX - 1) {
    return callback(null, false);
  }
  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var returned = packet.returnPacket(data);
    if (returned.code !== 0) {
      if (!callback.called) {
        callback.called = true;
        return callback(returned.code,false);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback(null, true);
    }
  });

};

/**
 * get  method
 * @type {Function}
 * @params key：must be string, the key to get
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err, data):
 * @params fitJava: true/ false, set to fit java key type
 * @params datatype: support string/buffer/float/double/int , returned data type, default is string.
 */
Tair.prototype.get = function (key, namespace, callback, fitJava, datatype) {
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof namespace === 'function') {
    datatype = fitJava
    fitJava = callback
    callback = namespace;
  }
  if (typeof fitJava !== 'boolean') {
    datatype = fitJava;
    fitJava = null;
  }
  datatype = datatype || 'string';

  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'get', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var _packet = packet.requestGetOrRemovePacket(_namespace, key, null, fitJava);

  var _addr = this.getDataNode(key, null, fitJava);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), null);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, null);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var res = packet.responseGetPacket(data);
    if(typeof res === 'number' )
    {
      if (!callback.called) {
        callback.called = true;
        return callback(res, null);
      }
      return
    }

    var d_list=utils.decodeBuffer(res.value,datatype)

    if (d_list === null ) {
      if (!callback.called) {
        callback.called = true;
        return callback( null,null);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback( null,d_list);
    }
  });
};

/**
 * mget  method
 * @type {Function}
 * @params keys：must be an array of string, the key to get
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err, data).
* @params datatype: support string/buffer/float/double/int , returned data type, default is string. 
 */
Tair.prototype.mget = function (keys, namespace, callback,datatype) {
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof namespace === 'function') {
    datatype = callback;
    callback = namespace;
  }
  if(typeof datatype==="undefined")
    datatype='string'

  if (!keys || keys.length === 0 || !Array.isArray(keys)) {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'mget', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var _keys = [];
  var _packets = {};
  for (var i = 0, l = keys.length; i < l; i++) {
    if (typeof keys[i] === 'string' && keys[i].length > 0 && keys[i].length < consts.NAMESPACE_MAX) {
      var _addr = this.getDataNode(keys[i]);
      var _addrKey = _addr.host + _addr.port;
      if (!_packets[_addrKey]) {
        _packets.length = (_packets.length || 0) + 1;
        _packets[_addrKey] = [];
        _packets[_addrKey].addr = _addr;
      }
      // 记录下每个服务器中发哪些包
      _packets[_addrKey].push(keys[i]);
    }
  }
  // 构建包
  // addr不会很多，用for..in..不会有性能问题
  var retCount = 0;
  var returned = {};
  var itemCount = 0;
  for (var k in _packets) {
    if (k === 'addr' || k === 'length') {
      continue;
    }
    var realkeys = [];
    for (var i = 0; i < _packets[k].length; i++) {
      realkeys.push(_packets[k][i]);
    }
    var _buf = packet.requestMGetPacket(_namespace, realkeys);
    comm.getData(_packets[k].addr, _buf, function (err, data, len) {
      if (err) {
        retCount++;
        if (retCount === _packets.length) {
          return callback(null, returned);
        }
        return;
      }
      var ret = packet.responseMGetPacket(data) || {};
      ret.count = ret.count || 0;
      for (var j = 0; j < ret.count; j++) {
        //returned[ret.keys[j]] = ret.values[j];
        returned[ret.keys[j]] = utils.decodeBuffer(ret.values[j],datatype);
        itemCount++;
      }
      retCount++;
      if (retCount === _packets.length) {
        returned.length = itemCount;

        return callback(null, returned);
      }
    });
  }

};

/**
 * remove  method
 * @type {Function}
 * @params key：must be string, the key to remove
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err):
 */
Tair.prototype.remove = Tair.prototype.delete = function (key, namespace, callback) {

  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof namespace === 'function') {
    callback = namespace;
  }

  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'remove', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var _packet = packet.requestGetOrRemovePacket(_namespace, key, true);

  var _addr = this.getDataNode(key);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), false);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      return callback(err, false);
    }
    var returned = packet.returnPacket(data);
    if (returned.code !== 0) {
      return callback(returned.code,false);
    }
    return callback(null);
  });
};

function incDec (key, count, namespace, initValue, expire, callback) {
  if (!key || key.length === 0) {
    return callback(new Error('Tair: params lack.'));
  }
  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'incDec', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }
  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, null);
  }
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  var _count = typeof count === 'number' ? count : 0;
  var _initValue = typeof initValue === 'number' ? initValue : 0;
  var _expire = typeof expire === 'number' ? expire : 0;
  if (typeof count === 'function') {
    callback = count;
  }
  if (typeof namespace === 'function') {
    callback = namespace;
  }
  if (typeof initValue === 'function') {
    callback = initValue;
  }
  if (typeof expire === 'function') {
    callback = expire;
  }

  var _packet = packet.requestIncDecPacket(_namespace, key, _count, _initValue, _expire);
  var _addr = this.getDataNode(key);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), false);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      return callback(err, false);
    }
    var returned = packet.returnPacket(data);
    if (returned.code < 0) {
      return callback(returned.code,false);
    }
    return callback(null, returned.code);
  });
}

/**
 * incr / decr  method
 * @type {Function}
 * @params key：must be string
 * @params count: amount to plus or minus, usually be positive number
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params initValue: if key is not exist, give it a value
 * @params expire: if key is not exist, the new value`s expire(seconds)
 * @params callback(err, data):
 */
Tair.prototype.incr = function (key, count, namespace, initValue, expire, callback) {
  count = count || 0;
  return incDec.apply(this, [key, count, namespace, initValue, expire, callback]);
};
Tair.prototype.decr = function (key, count, namespace, initValue, expire, callback) {
  count = count || 0;
  return incDec.apply(this, [key, -count, namespace, initValue, expire, callback]);
};

/**
 * smembers  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err, data):
 * @params datatype: 'string' or 'buffer', returned data type, default is string.
 */
Tair.prototype.smembers=function (key, namespace, callback, datatype)
{
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof namespace === 'function') {
    callback = namespace;
  }

  datatype = datatype || 'string';

  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'smembers', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var _packet = packet.requestWithOnlyKeyPacket(_namespace, key, consts.TAIR_REQ_SMEMBERS_PACKET);

  var _addr = this.getDataNode(key, null, null);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), null);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, null);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var res = packet.responseSmembersPacket(data);
    if(typeof res==='number')
    {
      if (!callback.called) {
        callback.called = true;
        return callback( res,null);
      }
      return ;
    }
    var d_list=utils.decodeBuffer(res,datatype)

    if (d_list ===null) {
      if (!callback.called) {
        callback.called = true;
        return callback( null,null);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback( null,d_list);
    }
  });
} 


/**
 * zrangebyscore  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params start: start of the score, double type, 8 bytes
 * @params end: end of the score, double type, 8 bytes
 * @params callback(err, data):
 * @params datatype: 'string' or 'buffer' or ..., returned data type, default is string.
 * @params limit: max length of result, TODO
 * @params with_score: whether or not return score, TODO
 */
Tair.prototype.zrangebyscore=function (key, namespace, start, end, callback, datatype, limit, with_score )
{
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof callback !=='function') {
    console.log("Tair zrangebyscore: params callback should be a function")
    return null
  }

  datatype = datatype || 'string';
  limit = limit || 0
  with_score =with_score || 0

  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'zrangebyscore', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var _packet = packet.requestZrangebyscorePacket(_namespace, key, start, end);

  var _addr = this.getDataNode(key, null, null);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), null);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(new Error('Tair: key.length bigger than consts.NAMESPACE_MAX.'), null);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var res = packet.responseZrangeCommonPacket(data);
    if(typeof res==='number')
    {
      if (!callback.called) {
        callback.called = true;
        return callback( res,null);
      }
      return ;
    }
      
    var d_list=utils.decodeBuffer(res,datatype)
    
    if (d_list ===null) {
      if (!callback.called) {
        callback.called = true;
        return callback( null,null);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback( null,d_list);
    }
  });
}

/**
 * zrange  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params start: start index, int type, 4 bytes
 * @params end: end index, int type, 4 bytes
 * @params callback(err, data):
 * @params datatype: 'string' or 'buffer' or..., returned data type, default is string.
 * @params with_score: whether or not return score, TODO
 */
Tair.prototype.zrange=function (key, namespace, start, end, callback, datatype, with_score )
{
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof callback !=='function') {
    console.log("Tair zrangebyscore: params callback should be a function")
    return null
  }

  datatype = datatype || 'string';
  with_score =with_score || 0

  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'zrange', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var _packet = packet.requestZrangePacket(_namespace, key, start, end, with_score);

  var _addr = this.getDataNode(key, null, null);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), null);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(new Error('Tair: key.length bigger than consts.NAMESPACE_MAX.'), null);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var res = packet.responseZrangeCommonPacket(data);
    if(typeof res==='number')
    {
      if (!callback.called) {
        callback.called = true;
        return callback( res,null);
      }
      return ;
    }
      
    var d_list=utils.decodeBuffer(res,datatype)
    
    if (d_list ===null) {
      if (!callback.called) {
        callback.called = true;
        return callback( null,null);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback( null,d_list);
    }
  });
}

/**
 * hset  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params field: usually string, the field to set
 * @params value: usually string, the value of field
 * @params callback(err, data)
 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
 */
Tair.prototype.hset=function (key, namespace, field, value, callback, expire, version) 
{
  var _expire = typeof expire === 'number' ? expire : 0;
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  var _version = typeof version === 'number' ? version : 0;
  if (typeof callback !=='function') {
    console.log("Tair zadd: params callback should be a function")
    return null
  }
  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair:zadd params - key lack.'), false);
  }
  if (field === null) {
    return callback(new Error('Tair:zadd params - field lack.'), false);
  }
  if (value === null) {
    return callback(new Error('Tair:zadd params - value lack.'), false);
  }
  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'hset', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }
  var _packet = packet.requestHsetPacket(key, _namespace, field, value, _expire, _version);
  var _addr = this.getDataNode(key);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), false);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, false);
  }
  if (value.length >= consts.TAIR_MALLOC_MAX - 1) {
    return callback(null, false);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var returned = packet.returnPacket(data);
    if (returned.code !== 0 && returned.code!== consts.TAIR_RETURN_ALREADY_EXIST) {
      if (!callback.called) {
        callback.called = true;
        return callback(returned.code,false);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback(null, true);
    }
  });
}



/**
 * zadd  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params value: usually string, the value to set
 * @params score: double type, 8 bytes
 * @params callback(err, data)
 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
 */
Tair.prototype.zadd=function (key, namespace, value, score, callback, expire, version) 
{
  var _expire = typeof expire === 'number' ? expire : 0;
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  var _version = typeof version === 'number' ? version : 0;
  if (typeof callback !=='function') {
    console.log("Tair zadd: params callback should be a function")
    return null
  }
  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair:zadd params - key lack.'), false);
  }
  if (value === null) {
    return callback(new Error('Tair:zadd params - value lack.'), false);
  }
  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'zadd', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }
  var _packet = packet.requestZaddPacket(key, _namespace, value, score, _expire, _version);
  var _addr = this.getDataNode(key);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), false);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, false);
  }
  if (value.length >= consts.TAIR_MALLOC_MAX - 1) {
    return callback(null, false);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var returned = packet.returnPacket(data);
    if (returned.code !== 0) {
      if (!callback.called) {
        callback.called = true;
        return callback(returned.code,false);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback(null, true);
    }
  });
}

/**
 * sadd  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params value: usually string, the value to set
 * @params callback(err, data)
 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
 */
Tair.prototype.sadd=function (key, namespace, value, callback, expire, version) 
{
  this.sadd_or_srem_or_zrem(key, namespace, value, callback, expire, version, consts.TAIR_REQ_SADD_PACKET) 
}

/**
 * srem  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params value: usually string, the value to remove
 * @params callback(err, data)
 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
 */
Tair.prototype.srem=function (key, namespace, value, callback, expire, version) 
{
  this.sadd_or_srem_or_zrem(key, namespace, value, callback, expire, version, consts.TAIR_REQ_SREM_PACKET) 
}

/**
 * zrem  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params value: usually string, the value to remove
 * @params callback(err, data)
 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
 */
Tair.prototype.zrem=function (key, namespace, value, callback, expire, version) 
{
  this.sadd_or_srem_or_zrem(key, namespace, value, callback, expire, version, consts.TAIR_REQ_ZREM_PACKET) 
}


/**
 * sadd_or_srem_or_zrem  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, must set here
 * @params value: usually string, the value to set
 * @params callback(err, data)
 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
 * @params packet_type: request package type
 */
Tair.prototype.sadd_or_srem_or_zrem=function (key, namespace, value, callback, expire, version, packet_type) 
{
  var _expire = typeof expire === 'number' ? expire : 0;
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  var _version = typeof version === 'number' ? version : 0;
  if (typeof callback !=='function') {
    console.log("Tair sadd_or_srem_or_zrem: params callback should be a function")
    return null
  }
  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair:sadd_or_srem_or_zrem params - key lack.'), false);
  }
  if (value === null) {
    return callback(new Error('Tair:sadd_or_srem_or_zrem params - value lack.'), false);
  }
  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'sadd_or_srem_or_zrem', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }
  var _packet = packet.requestWithKeyAndValuePacket(key, _namespace, value, _expire, _version, packet_type);
  var _addr = this.getDataNode(key);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), false);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, false);
  }
  if (value.length >= consts.TAIR_MALLOC_MAX - 1) {
    return callback(null, false);
  }  

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var returned = packet.returnPacket(data);
    if (returned.code !== 0) {
      if (!callback.called) {
        callback.called = true;
        return callback(returned.code,false);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback(null, true);
    }
  });
}

/**
 * scard method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err, data):
 */
Tair.prototype.scard=function (key, namespace, callback)
{
  this.scard_or_zcard(key, namespace, callback, consts.TAIR_REQ_SCARD_PACKET)
}

/**
 * zcard method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err, data):
 */
Tair.prototype.zcard=function (key, namespace, callback)
{
  this.scard_or_zcard(key, namespace, callback, consts.TAIR_REQ_ZCARD_PACKET)
}

/**
 * scard_or_zcard  method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err, data):
 * @params packet_type: request package type 
 */
Tair.prototype.scard_or_zcard=function (key, namespace, callback, packet_type)
{
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof namespace === 'function') {
    callback = namespace;
  }

  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'scard_or_zcard', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var _packet = packet.requestWithOnlyKeyPacket(_namespace, key, packet_type);

  var _addr = this.getDataNode(key, null, null);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), null);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, null);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var res = packet.responseWithInt64Packet(data);
    if(typeof res==='number')
    {
      if (!callback.called) {
        callback.called = true;
        return callback( res,null);
      }
      return ;
    }
    var d_list=utils.decodeBuffer(res,"long")

    if (d_list===null) {
      if (!callback.called) {
        callback.called = true;
        return callback( null,null);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback( null,d_list);
    }
  });
} 


/**
 * hgetall method
 * @type {Function}
 * @params key：must be string
 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
 * @params callback(err, data):
 */
Tair.prototype.hgetall=function (key, namespace, callback)
{
  var _namespace = typeof namespace === 'number' ? namespace : 0;
  if (typeof namespace === 'function') {
    callback = namespace;
  }

  if (!key || key.length === 0 || typeof key !== 'string') {
    return callback(new Error('Tair: params lack.'), false);
  }

  if (!this.inited) {
    //console.warn('Tair: client not init');
    this.requestQueue.push({method: 'hgetall', args: Array.prototype.slice.apply(arguments), that: this});
    return;
  }

  var packet_type=consts.TAIR_REQ_HGETALL_PACKET

  var _packet = packet.requestWithOnlyKeyPacket(_namespace, key, packet_type);

  var _addr = this.getDataNode(key, null, null);
  if (!_addr.success) {
    return callback(new Error('Tair: find datanode error'), null);
  }

  if (key.length >= consts.NAMESPACE_MAX) {
    return callback(null, null);
  }

  comm.getData(_addr, _packet, function (err, data, len) {
    if (err) {
      if (!callback.called) {
        callback.called = true;
        return callback(err, false);
      }
    }
    var res = packet.responseHgetallPacket(data);
    if(typeof res==='number')
    {
      if (!callback.called) {
        callback.called = true;
        return callback( res,null);
      }
      return ;
    }
    
    if (res===null) {
      if (!callback.called) {
        callback.called = true;
        return callback( null,null);
      }
    }
    if (!callback.called) {
      callback.called = true;
      return callback( null,res);
    }
  });
} 
