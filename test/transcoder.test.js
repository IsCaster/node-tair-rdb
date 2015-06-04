/*
 * tair - test/transcoder.test.js
 * Copyright(c) 2012 Taobao.com
 * Author: kate.sf <kate.sf@taobao.com>
 */

var trans = require('../lib/transcoder');
var should = require('should');

describe('transcoder.test.js', function () {
  it('should encode and decode number 0 right', function () {
    var bool =0;
    var enc = trans.encode(bool,'utf-8',true) ;
    enc.should.length(6);
    var dec = trans.decode(enc);
  });
  it('should encode and decode number right', function () {
    var bool =123456;
    var enc = trans.encode(bool,'utf-8',true) ;
    enc.should.length(6);
    var dec = trans.decode(enc);
  });
  it('should encode and decode date right', function () {
    var bool = new Date();
    var enc = trans.encode(bool, 'utf-8', true);
    enc.should.length(6);
    var dec = trans.decode(enc);
  });
  it('should encode and decode float right', function () {
    var bool = 12345.678;
    var enc = trans.encode(bool, 'utf-8', true);
    enc.should.length(10);
    var dec = trans.decode(enc);
  });
  it('should encode and decode string right', function () {
    var bool = 'i am a string';
    var enc = trans.encode(bool, 'utf-8', true);
    enc.should.length(2 + bool.length);
    var dec = trans.decode(enc);
  });
  it('should encode and decode empty string right', function () {
    var bool = '';
    var enc = trans.encode(bool, 'utf-8', true);
    enc.should.length(2 + bool.length);
    var dec = trans.decode(enc);
  });
  it('should encode and decode boolean right', function () {
    var bool = true;
    var enc = trans.encode(bool, 'utf-8', true);
    enc.should.length(3);
    var dec = trans.decode(enc);
  });
  it('should return null when no obj input', function () {
    var enc = trans.encode(null, 'utf-8', true);
    should.not.exist(enc);
  });
});
