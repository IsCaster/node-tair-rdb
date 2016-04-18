/*!
 * tair - lib/packet.js
 * Copyright(c) 2012 Taobao.com
 * Author: kate.sf <kate.sf@taobao.com>
 */

/**
 * network communication module
 */

var net = require('net');
var consts = require('./const');
var Cutter = require('cutter');
var co = require('co')
var promisify =  require('bluebird').promisify



function ClientPool(addr,timeout){
  this.client_list=[]
  this.addr=addr
  this.timeout=timeout
}

ClientPool.prototype.newClient=function(num){
  var that=this
  return co(function*(){
    for(var i=0;i<num;++i){
      var client = new net.Socket();
      client.setTimeout(this.timeout);
      var q_connect=promisify(client.connect, {context:client})
      yield q_connect(this.addr.port, this.addr.host)
      client.on('timeout', function () {
        client.end(new Buffer(' '));
      });
      client.on('error', function (err) {
        client.end(new Buffer(' '));
      });

      var cutter = new Cutter(16, packetLength);
      client.cutter = cutter
      client.callback = function(){}
      cutter.on('packet', function (chunk) {
        client.callback(null, chunk, chunk.length);
        client.valid=true
      });
      client.on('data', function (data) {
        cutter.emit('data', data);
      });
      client.on('end', function () {
        client.valid=false
        client.cutter.destroy();
        client.distroy()
        var index=that.client_list.indexOf(client)
        that.client_list.splice(index,1)
      });
      client.valid=true
      this.client_list.push(client)
    }
  })
}


ClientPool.prototype._getValidClient=function(){
  var valid_client=null
  for(client of this.client_list)
  {
    if(client.valid)
    {
      client.valid=false
      valid_client=client
      break;
    }
  }
  return valid_client
}

ClientPool.prototype.getValidClient=function(){
  return co(function*(){
    var valid_client=this._getValidClient()

    if(!valid_client)
    {
      valid_client=this.newClient(10).then(function(){
        return this.getValidClient()
      })
    }
    return valid_client
  }).catch(function(err){
    console.error("ClientPool.prototype.getValidClient() err="+err.stack)
    return null
  })
}

var client_pool_list={}
exports.globalTimeout = 3000;
function get_client_pool(addr){
  var key=addr.host+":"+addr.port
  if(client_pool_list[key])
  {
    return client_pool_list[key] 
  }
  else
  {
    client_pool_list[key]=new ClientPool(addr,exports.globalTimeout) 
  }
}

exports.getData = function (addr, buf, callback) {
  co(function*(){
    var client_pool=get_client_pool(addr)
    var client=yield client_pool.getValidClient()
    client.callback=callback
    client.write(buf);
  })
};

function packetLength (data) {
  return 16 + data.readInt32BE(12);
}

function printBuffer (buf) {
  for (var i = 0; i < buf.length; i++) {
    if (i % 8 === 0) process.stdout.write(' ');
    if (i % 24 === 0) process.stdout.write('\n');
    process.stdout.write((buf[i] < 16 ? '0' : '') + buf[i].toString(16) + ' ');
  }
  process.stdout.write('\n');

}

exports.printBuffer = printBuffer;
