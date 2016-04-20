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
  this.connecting_clients_counter=0
  this.addr=addr
  this.timeout=timeout
}

function default_callback(err)  
{
  if(err)
    console.error("default_callback() , err="+(err.stack)?err.stack:err)
}

ClientPool.prototype.newClient=function(){
  var that=this
  return co(function*(){
    var client = new net.Socket();
    client.setTimeout(that.timeout);
    var q_connect=promisify(client.connect, {context:client})
    client.on('timeout', function () {
      client.end(new Buffer(' '));
      if(client.valid)
      {
        client.valid=false
        return null
      }
      else
      {
        return client.callback(new Error('Tair: Server ' + that.addr.host +":"+ that.addr.port + ' Timeout'));
      }
    });
    client.on('error', function (err) {
      //client.end(new Buffer(' '));
      that.removeClient(client)
      if(client.valid)
      {
        client.valid=false
        return null
      }
      else
      {
        if(! client.callback || !(client.callback instanceof Function)){
          client.callback=default_callback
        }
        return client.callback(err || new Error('Tair: Server ' + that.addr.host+":"+ that.addr.port + ' Error.'));
      }
    });

    var cutter = new Cutter(16, packetLength);
    
    cutter.on('packet', function (chunk) {
      client.callback(null, chunk, chunk.length);
      client.callback=default_callback
      client.valid=true
    });
    client.on('data', function (data) {
      cutter.emit('data', data);
    });
    client.on('end', function () {
      client.valid=false
      cutter.destroy();
      that.removeClient(client)
    });
    client.valid=true
    yield q_connect(that.addr.port, that.addr.host)
    client.callback=default_callback
    that.client_list.push(client)
  })
}
ClientPool.prototype.removeClient=function(client){
  client.destroy()
  var index=that.client_list.indexOf(client)
  if(index>=0)
    that.client_list.splice(index,1)
}

ClientPool.prototype.newBatchClients=function(num){
  var that=this
  return co(function*(){
    for(var i=0;i<num;++i)
    {
      yield that.newClient()
    }
  })
}

ClientPool.prototype._getValidClient =function(){
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

ClientPool.prototype.wait=function(time){
  return new Promise(function(resolve,reject){
    setTimeout(function(){
      resolve()
    },time)
  })
}

ClientPool.prototype.getValidClient=function(counter){
  var that=this
  if(!counter)
    counter=1
  return co(function*(){
    var valid_client=that._getValidClient()

    if(!valid_client)
    {
      if(that.client_list.length<1000 && that.connecting_clients_counter<=30)
      {
        that.connecting_clients_counter=that.connecting_clients_counter+10
        yield that.newBatchClients(5)
        that.connecting_clients_counter=that.connecting_clients_counter-10
        return that.getValidClient(counter+1)
      }
      else
      {
        yield that.wait(100＊counter)
        return that.getValidClient(counter+1)
      }
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
    return client_pool_list[key]
  }
}

exports.getData = function (addr, buf, callback) {
  //console.log("enter getData()")
  co(function*(){
    var client_pool=get_client_pool(addr)
  //console.log("client_pool="+client_pool)
    var client=yield client_pool.getValidClient()
  //console.log("client="+require("util").inspect(client))
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