/*!
 * tair - lib/packet.js
 * Copyright(c) 2012 Taobao.com
 * Author: kate.sf <kate.sf@taobao.com>
 */

/**
 * network packet structure
 */

var consts = require('./const');
var zlib = require('zlib');
var utils = require('./utils');
var transcoder = require('./transcoder');
var printbuf = require('./comm').printBuffer;

var chid = 1; //global channel id

exports.writePacketBegin = function (pcode, capacity) {

  capacity = capacity || 0;
  chid++;
  // fix chid crashed to make 0
  if (chid >= 300000) {
    chid = 1;
  }

  // packet header
  var byteBuffer = new Buffer(capacity + 16);
  byteBuffer.fill();
  byteBuffer.writeUInt32BE(consts.TAIR_PACKET_FLAG, 0); // packet flag
  // 终极保险, chid决不能为0
  if (chid <= 0) {
    chid = parseInt(Math.random() * 10000, 10) + 10;
    console.warn('Warning: Tair chid make to 0!!!! (1)');
  }
  byteBuffer.writeUInt32BE(chid, 4); // channel id
  byteBuffer.writeUInt32BE(pcode, 8); // packet code
  byteBuffer.writeUInt32BE(capacity, 12); // body len
  return [byteBuffer, 16];
};

exports.writePacketEnd = function (byteBuffer, pos) {
  byteBuffer.writeInt8(consts.TAIR_PACKET_HEADER_BLPOS, pos);
};

exports.requestGetGroupPacket = function (groupName, configVersion) {
  configVersion = configVersion || 0;
  var ret = exports.writePacketBegin(consts.TAIR_REQ_GET_GROUP_NEW_PACKET, 9 + groupName.length);
  var buf = ret[0], pos = ret[1];
  buf.writeInt32BE(configVersion, pos);
  pos += 4;
  pos += writeString(buf, pos, groupName);
  return buf;
};

exports.responseGetGroupPacket = function (data, callback) {

  if (!data) {
    return callback(new Error('Tair: Config Server return Empty Data'));
  }

  var pos = 16;
  var bucketCount = data.readInt32BE(pos);
  pos += 4;
  var copyCount = data.readInt32BE(pos);
  pos += 4;
  var configVersion = data.readInt32BE(pos);
  pos += 4;
  var configCount = data.readInt32BE(pos);
  pos += 4;
  var configMap = {};

  for (var i = 0; i < configCount; i++) {
    var name = readString(data, pos);
    pos += name[1];

    var value = readString(data, pos);
    pos += value[1];
    configMap[name[0]] = value[0];
  }

  var serverList = [];
  var serverCountFlate = data.readInt32BE(pos);
  pos += 4;
  if (serverCountFlate > 0) {
    var _buff = new Buffer(serverCountFlate);
    data.copy(_buff, 0, pos, pos + serverCountFlate);
    zlib.inflate(_buff, function (err, buffer) {
      if (err) {
        return callback(err);
      }
      pos += serverCountFlate;
      var posTemp = 0;
      var countTemp = 0;
      var copyTemp = 0;
      while (posTemp < buffer.length) {
        if (countTemp === 0) {
          serverList[copyTemp] = [];
        }
        serverList[copyTemp].push({host: buffer.readInt32LE(posTemp) >>> 0, port: buffer.readInt32LE(posTemp + 4)});
        posTemp += 8;
        countTemp++;
        if (countTemp === bucketCount) {
          countTemp = 0;
          copyTemp++;
        }
      }
      var aNodeCount = data.readInt32BE(pos);
      pos += 4;
      var aliveNode = {};
      for (var i = 0; i < aNodeCount; i++) {
        aliveNode[readLong(data, pos)] = true;
        pos += 8;
      }
      var configServerRes = {
        configVersion: configVersion,
        bucketCount: bucketCount,
        copyCount: copyCount,
        configMap: configMap,
        serverList: serverList,
        aliveNode: aliveNode
      };
      //console.log(configServerRes);
      return callback(null, configServerRes);
    });
  } else {
    return callback(new Error('Tair: No Server'));
  }

};

exports.requestPutPacket = function (namespace, version, key, value, expire) {
  var keyBuffer = transcoder.encode(key);
  var valueBuffer = transcoder.encode(value);
  var ret = exports.writePacketBegin(consts.TAIR_REQ_PUT_PACKET, 90 + keyBuffer.length + valueBuffer.length);
  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt16BE(version, pos);
  pos += 2;
  packet.writeInt32BE(expire, pos);
  pos += 4;
  //key
  pos += fillMeta(packet, pos);
  pos += encodeMeta(packet, pos, 0);
  packet.writeInt32BE(keyBuffer.length + 1, pos);
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length + 1;
  //value
  pos += fillMeta(packet, pos);
  pos += encodeMeta(packet, pos, 0);
  packet.writeInt32BE(valueBuffer.length, pos);
  pos += 4;
  valueBuffer.copy(packet, pos, 0, valueBuffer.length);
  pos += valueBuffer.length + 1;

  return packet;
};

exports.requestGetOrRemovePacket = function (namespace, key, remove, fitJava) {
  var _keyBuffer = transcoder.encode(key);
  if (fitJava) {
    var keyBuffer = new Buffer(_keyBuffer.length + 1);
    keyBuffer[0] = 0;
    _keyBuffer.copy(keyBuffer, 1);
  } else {
    var keyBuffer = _keyBuffer;
  }
  if (!remove) {
    var ret = exports.writePacketBegin(consts.TAIR_REQ_GET_PACKET, 48 + keyBuffer.length);
  } else {
    var ret = exports.writePacketBegin(consts.TAIR_REQ_REMOVE_PACKET, 48 + keyBuffer.length);
  }
  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt32BE(1, pos);     // key list size
  pos += 4;
  //key
  pos += fillMeta(packet, pos);
  pos += encodeMeta(packet, pos, 0);
  var zeroOffset = fitJava ? 0 : 1;
  packet.writeInt32BE(keyBuffer.length + zeroOffset, pos);
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length + zeroOffset;

  return packet;
};

exports.requestMGetPacket = function (namespace, keys) {
  var kCount = keys.length;
  var kSize = 0;
  var bufList = [];
  for (var i = 0; i < kCount; i++) {
    bufList[i] = transcoder.encode(keys[i]);
    kSize += bufList[i].length;
  }
  var ret = exports.writePacketBegin(consts.TAIR_REQ_GET_PACKET, 7 + kSize + kCount * 41);
  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt32BE(kCount, pos);     // key list size
  pos += 4;
  for (var i = 0; i < kCount; i++) {
    pos += fillMeta(packet, pos);
    pos += encodeMeta(packet, pos, 0);
    packet.writeInt32BE(bufList[i].length + 1, pos);
    pos += 4;
    bufList[i].copy(packet, pos, 0, bufList[i].length);
    pos += bufList[i].length + 1;
  }
  return packet;
};

exports.responseMGetPacket = function (data) {
  if (!data) {
    return {key: null, value: null};
  }
  var pos = 16;
  if (data.length < 19) {
    return null;
  }
  var configVer = data.readInt32BE(pos);
  pos += 4;
  var result = data.readInt32BE(pos);
  pos += 4;
  if (result !== -3983 && result!==0) {
    console.log("responseMGetPacket result="+result+' , return null')
    return null;
  }
  var vCount = data.readInt32BE(pos);
  pos += 4;
  if (vCount === 0) {
    return null;
  }
  var keys = [];
  var values = [];
  for (var i = 0; i < vCount; i++) {
    pos += 36; // we donnot need meta data
    var kSize = data.readInt32BE(pos);
    pos += 4;
    var kBuf = new Buffer(kSize);
    data.copy(kBuf, 0, pos, pos + kSize);
    pos += kSize;
    pos += 36; // we donnot need meta data
    var vSize = data.readInt32BE(pos);
    pos += 4;
    var vBuf = new Buffer(vSize);
    data.copy(vBuf, 0, pos, pos + vSize);
    pos += vSize;
    keys.push(kBuf.toString('utf-8').replace('\u0000', ''));
    //values.push(vBuf.toString('utf-8').replace('\u0000', ''));
    values.push(vBuf);
  }
  return {keys: keys, values: values, count: vCount};
};

exports.responseGetPacket = function (data) {
  if (!data) {
    return {key: null, value: null};
  }
  var pos = 16;
  if (data.length < 19) {
    return null;
  }
  var configVer = data.readInt32BE(pos);
  pos += 4;
  var result = data.readInt32BE(pos);
  pos += 4;
  if (result !== 0) {
    console.log("responseGetPacket return "+result)
    return result;
  }
  var vCount = data.readInt32BE(pos);
  pos += 4;
  if (vCount > 1) {
    console.log('Tair: warn: this method cannot support multi get, use mget instead.');
  }
  if (vCount === 0) {
    return null;
  }
  pos += 36; // we donnot need meta data
  var kSize = data.readInt32BE(pos);
  pos += 4;
  var kBuf = new Buffer(kSize);
  data.copy(kBuf, 0, pos, pos + kSize);
  pos += kSize;
  pos += 36; // we donnot need meta data
  var vSize = data.readInt32BE(pos);
  pos += 4;
  var vBuf = new Buffer(vSize);
  data.copy(vBuf, 0, pos, pos + vSize);
  pos += vSize;

  return {key: kBuf.toString('utf-8').replace('\u0000', ''), value: vBuf};
};

exports.requestIncDecPacket = function (namespace, key, count, initValue, expire) {
  var keyBuffer = transcoder.encode(key);

  var ret = exports.writePacketBegin(consts.TAIR_REQ_INCDEC_PACKET, 56 + keyBuffer.length);

  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt32BE(count || 0, pos);
  pos += 4;
  packet.writeInt32BE(initValue || 0, pos);
  pos += 4;
  packet.writeInt32BE(expire || 0, pos);
  pos += 4;

  //key
  pos += fillMeta(packet, pos);
  pos += encodeMeta(packet, pos, 0);
  packet.writeInt32BE(keyBuffer.length + 1, pos);
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length + 1;

  return packet;
};

exports.requestWithOnlyKeyPacket = function (namespace, key, packet_type) {
  var keyBuffer = transcoder.encode(key);  
  var ret = exports.writePacketBegin(packet_type, 8 + keyBuffer.length);

  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt32BE(keyBuffer.length+1, pos);     // key size
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length;
  packet[pos]=0;
  pos ++;
  return packet;
};

exports.responseSmembersPacket = function (data) {
  if (!data) {
    return {key: null, value: null};
  }
  var pos = 16;
  if (data.length < 19) {
    return null;
  }
  var configVer = data.readInt32BE(pos);
  pos += 4;
  var version = data.readInt16BE(pos)
  pos += 2;
  var code = data.readInt32BE(pos);
  pos += 4;
  if (code !== 0) {
    console.log("responseSmembersPacket return ,code="+code)
    return code;
  }

  var vlen = data.readInt32BE(pos);
  pos += 4;
  if (vlen === 0) {
    console.log("responseSmembersPacket return ,variable length="+vlen)
    return null;
  }

  var dlen=0
  var d_list=[]
  for(var i=0;i<vlen;++i)
  {
    dlen=data.readInt32BE(pos)
    pos=pos+4
    if(dlen === 0)
    {
      continue
    }
    var vBuf = new Buffer(dlen);
    data.copy(vBuf, 0, pos, pos + dlen);
    d_list.push(vBuf)
    pos=pos+dlen
  }
  return d_list
};

exports.requestZrangebyscorePacket = function (namespace, key, start, end) {
  var keyBuffer = transcoder.encode(key);  
  var ret = exports.writePacketBegin(consts.TAIR_REQ_ZRANGEBYSCORE_PACKET, 24 + keyBuffer.length);

  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;

  packet.writeDoubleLE(start, pos);
  pos += 8
  packet.writeDoubleLE(end, pos);
  pos += 8

  packet.writeInt32BE(keyBuffer.length+1, pos);     // key size
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length;
  packet[pos]=0;
  pos ++;
  return packet;
};

exports.requestZrangePacket = function (namespace, key, start, end, with_score) {
  var keyBuffer = transcoder.encode(key);  
  var ret = exports.writePacketBegin(consts.TAIR_REQ_ZRANGE_PACKET, 20 + keyBuffer.length);

  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;

  packet.writeInt32BE(start, pos);
  pos += 4
  packet.writeInt32BE(end, pos);
  pos += 4

  packet.writeInt32BE(with_score, pos);
  pos += 4

  packet.writeInt32BE(keyBuffer.length+1, pos);     // key size
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length;
  packet[pos]=0;
  pos ++;
  return packet;
};

exports.responseZrangeCommonPacket = function (data) {
  if (!data) {
    return {key: null, value: null};
  }
  var pos = 16;
  if (data.length < 19) {
    return null;
  }
  var configVer = data.readInt32BE(pos);
  pos += 4;
  var version = data.readInt16BE(pos)
  pos += 2;
  var code = data.readInt32BE(pos);
  pos += 4;
  if (code !== 0) {
    console.log("responseZrangeCommonPacket return ,code="+code)
    return code;
  }

  var vlen = data.readInt32BE(pos);
  pos += 4;
  if (vlen === 0) {
    console.log("responseZrangeCommonPacket return ,variable length="+vlen)
    return null;
  }

  var dlen=0
  var d_list=[]
  for(var i=0;i<vlen;++i)
  {
    dlen=data.readInt32BE(pos)
    pos=pos+4
    if(dlen === 0)
    {
      continue
    }
    var vBuf = new Buffer(dlen);
    data.copy(vBuf, 0, pos, pos + dlen);
    d_list.push(vBuf)
    pos=pos+dlen
  }
  return d_list
};

exports.requestZaddPacket = function(key, namespace, value, score, expire, version)
{
  var keyBuffer = transcoder.encode(key);
  var valueBuffer = transcoder.encode(value);
  var ret = exports.writePacketBegin(consts.TAIR_REQ_ZADD_PACKET, 26 + keyBuffer.length + valueBuffer.length);
  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt16BE(version, pos);
  pos += 2;
  packet.writeInt32BE(expire, pos);
  pos += 4;
  packet.writeDoubleLE(score, pos);
  pos += 8;
  //key
  packet.writeInt32BE(keyBuffer.length + 1, pos);
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length;
  packet[pos]=0
  pos++;
  //value
  packet.writeInt32BE(valueBuffer.length, pos);
  pos += 4;
  valueBuffer.copy(packet, pos, 0, valueBuffer.length);
  pos += valueBuffer.length+1;
  return packet;
}

exports.requestHsetPacket = function(key, namespace, field, value, expire, version)
{
  var keyBuffer = transcoder.encode(key);
  var fieldBuffer = transcoder.encode(field);
  var valueBuffer = transcoder.encode(value);
  var ret = exports.writePacketBegin(consts.TAIR_REQ_HSET_PACKET, 22 + keyBuffer.length + fieldBuffer.length + valueBuffer.length);
  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt16BE(version, pos);
  pos += 2;
  packet.writeInt32BE(expire, pos);
  pos += 4;
  //key
  packet.writeInt32BE(keyBuffer.length + 1, pos);
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length;
  packet[pos]=0
  pos++;
  //field
  packet.writeInt32BE(fieldBuffer.length, pos);
  pos += 4;
  fieldBuffer.copy(packet, pos, 0, fieldBuffer.length);
  pos += fieldBuffer.length;
  //value
  packet.writeInt32BE(valueBuffer.length, pos);
  pos += 4;
  valueBuffer.copy(packet, pos, 0, valueBuffer.length);
  pos += valueBuffer.length;
  return packet;
}

exports.requestWithKeyAndValuePacket = function(key, namespace, value, expire, version, packet_type)
{
  var keyBuffer = transcoder.encode(key);
  var valueBuffer = transcoder.encode(value);
  var ret = exports.writePacketBegin(packet_type, 18 + keyBuffer.length + valueBuffer.length);
  var pos = ret[1], packet = ret[0];
  //packet body begin
  packet[pos] = 0;
  pos++;
  packet.writeInt16BE(namespace, pos);
  pos += 2;
  packet.writeInt16BE(version, pos);
  pos += 2;
  packet.writeInt32BE(expire, pos);
  pos += 4;
  //key
  packet.writeInt32BE(keyBuffer.length + 1, pos);
  pos += 4;
  keyBuffer.copy(packet, pos, 0, keyBuffer.length);
  pos += keyBuffer.length;
  packet[pos]=0
  pos++;
  //value
  packet.writeInt32BE(valueBuffer.length, pos);
  pos += 4;
  valueBuffer.copy(packet, pos, 0, valueBuffer.length);
  pos += valueBuffer.length;
  return packet;
}

exports.responseWithInt64Packet = function (data) {
  var pos = 16;
  if (!data) return {configVersion: 0, code: -1001};
  if (data.length < 19) {
    return {configVersion: 0, code: -1000};
  }
  var configVer = data.readInt32BE(pos);
  pos += 4;
  var code = data.readInt32BE(pos);
  pos += 4;
  if(code !=0)
  {
    console.log("responseWithInt64Packet return ,code="+code)
    return code
  }
  var retNumBuf = new Buffer(8);
  data.copy(retNumBuf, 0, pos, pos + 8);
  return retNumBuf
}


exports.responseHgetallPacket = function (data) {
  if (!data) {
    return {key: null, value: null};
  }
  var pos = 16;
  if (data.length < 19) {
    return null;
  }
  var configVer = data.readInt32BE(pos);
  pos += 4;
  var version = data.readInt16BE(pos)
  pos += 2;
  var code = data.readInt32BE(pos);
  pos += 4;
  if (code !== 0) {
    console.log("responseHgetallPacket return ,code="+code)
    return code;
  }

  var vlen = data.readInt32BE(pos);
  pos += 4;
  if (vlen === 0) {
    console.log("responseHgetallPacket return ,variable length="+vlen)
    return null;
  }

  var dlen=0
  var d_list=[]
  var obj={}

  function clone(obj) {
    if (null == obj || "object" != typeof obj) return obj;
    var copy = obj.constructor();
    for (var attr in obj) {
        if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
    }
    return copy;
  }

  for(var i=0;i<vlen;++i)
  {
    dlen=data.readInt32BE(pos)
    pos=pos+4
    if(dlen === 0)
    {
      continue
    }
    var vBuf = new Buffer(dlen);
    data.copy(vBuf, 0, pos, pos + dlen);
    //d_list.push(vBuf)
    if(i%2===0)
    {
      obj.field=utils.decodeBuffer(vBuf,"string")
    }
    else
    {
      obj.value=utils.decodeBuffer(vBuf,"string")
      d_list.push(clone(obj))
    }
    pos=pos+dlen
  }
  return d_list
};


exports.returnPacket = function (data) {
  var pos = 16;
  if (!data) return {configVersion: 0, code: -1001};
  if (data.length < 19) {
    return {configVersion: 0, code: -1000};
  }
  var configVer = data.readInt32BE(pos);
  pos += 4;
  var result = data.readInt32BE(pos);
  return {configVersion: configVer, code: result};
}

function readLong (buffer, pos) {
  var _buf = new Buffer(8);
  buffer.copy(_buf, 0, pos, pos + 8);
  return utils.decodeLong(_buf);
}

var writeString = exports.writeString = function (byteBuffer, pos, str) {
  var written = 0;
  if (str == null) {
    byteBuffer.writeInt32BE(0, pos);
    written += 4;
  } else {
    var _buf = new Buffer(str, 'utf-8');
    byteBuffer.writeInt32BE(_buf.length + 1, pos);
    written += 4;
    written += byteBuffer.write(str, pos + written, 'utf-8');
    byteBuffer.writeInt8(0, pos + written);
    written += 4;
  }
  return written;
}

var readString = exports.readString = function (byteBuffer, pos) {
  var len = parseInt(byteBuffer.readInt32BE(pos));
  if (len <= 1) {
    return "";
  } else {
    var b = new Buffer(len);
    byteBuffer.copy(b, 0, pos + 4, pos + 4 + len);
    return [b.toString('utf-8').replace('\u0000', ''), 4 + len];
  }
}

function encodeMeta (buffer, pos, flag) {
  for (var i = pos; i < pos + 29; i++) {
    buffer[i] = 0;
  }
  if (flag !== 0) {
    buffer.writeInt32BE(flag, pos + 13);
  }
  return 29;
}
function fillMeta (buffer, pos) {
  for (var i = pos; i < pos + 7; i++) {
    buffer[i] = 0;
  }
  return 7;
}
