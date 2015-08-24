/*
 * tair - test/client.test.js
 * Copyright(c) 2012 Taobao.com
 * Author: kate.sf <kate.sf@taobao.com>
 */

var cli = require('../lib/client.js');
var should = require('should');
var fs = require('fs');

var tair;
var nm=2;
describe('client.test.js', function () {

  before(function (done) {
    tair = new cli('group_1', [
      {host: 'localhost', port: 5198}
    ], {heartBeatInterval: 3000},
      function (err) {
        if (err) {
          done(err);
          return;
        }
        done();
      });
  });

  it('#set method should set a data', function (done) {
    tair.set('xiang', 'we are testers',0,2,0,function (err, success) {
      should.not.exist(err);
      success.should.equal(true);
      done();
    });
  });

  it('#get method should get right data', function (done) {
    tair.get('xiang',2, function (err, data) {
      should.not.exist(err);
      data.should.equal('we are testers');
      done();
    });
  });
  it('#set method should set empty string right', function (done) {
    tair.set('xiang', '',0,2,0,function (err, success) {
      should.not.exist(err);
      success.should.equal(true);
      done();
    });
  });

  it('#get method should get empty string right', function (done) {
    tair.get('xiang',2, function (err, data) {
      should.not.exist(err);
      data.should.equal('');
      done();
    });
  });

  it('#set method should set int 0 right', function (done) {
    tair.set('xiang', 0,0,2,0,function (err, success) {
      should.not.exist(err);
      success.should.equal(true);
      done();
    });
  });

  it('#get method should get int 0 right', function (done) {
    tair.get('xiang',2, function (err, data) {
      should.not.exist(err);
      data.should.equal(0);
      done();
    },datatype='int');
  });


  it('#get and #set method should get buffer data when datatype is buffer', function (done) {
    tair.set('unittestjs', new Buffer('we are testers'),0,nm,0, function (err, success) {
      tair.get('unittestjs', nm, function (err, data) {
        should.not.exist(err);
        should.exist(data);
        should.ok(Buffer.isBuffer(data));
        done();
      }, false, 'buffer');
    });
  });

  it('#get and #set method should get float when datatype is float', function (done) {
    tair.set('unittestjs', 3.14159,0,nm,0, function (err, success) {
      tair.get('unittestjs', nm, function (err, data) {
        should.not.exist(err);
        should.exist(data);
        data.should.equal(3.14159);
        done();
      }, false, 'float');
    });
  });

  it('#get method should get empty data when key is wrong', function (done) {
    tair.get('zhemechangniyoume',2,function (err, data) {
      err.should.equal(-3998);
      should.not.exist(data);
      done();
    });
  });

  it('#remove method should remove data right', function (done) {
    tair.set('unittestjs', 'we are testers', function (err, success) {
      should.not.exist(err);
      success.should.equal(true);
      tair.remove('unittestjs', function (err) {
        should.not.exist(err);
        tair.get('unittestjs', function (err, data) {
          err.should.equal(-3998);
          should.not.exist(data);
          done();
        });
      });
    });
  });

  it('#set and #get will work well on large data', function (done) {
    var content = fs.readFileSync('./large_text.txt', 'utf-8');
    tair.set('alargeData', content,0,nm,0, function (err, success) {
      should.not.exist(err);
      success.should.equal(true);
      tair.get('alargeData',nm , function (err, data) {
        should.not.exist(err);
        data.should.equal(content);
        done();
      });
    });
  });

  it('#incr and decr will work well', function (done) {
    var keyName = 'incrTestKey' + new Date().getTime();
    tair.incr(keyName, 1,nm ,function (err, data) {
      should.not.exist(err);
      data.should.equal(1);

      tair.decr(keyName, 1,nm, function (err, data) {
        should.not.exist(err);
        data.should.equal(0);
        tair.incr(keyName, 0, nm,function (err, data) {
          should.not.exist(err);
          data.should.equal(0);
          done();
        });
      });
    });
  });

  it('heartbeat should work', function (done) {
    tair.heartbeat(function (count) {
      count.should.above(0);
      count.should.below(100);
    });
    setTimeout(function () {
      tair.heartBeatCount.should.equal(2);
      done();
    }, 500);
  });

  it("zadd string should work",function(done){
    var zkey="zadd.test.key";
    var zadd_values=['zvalue1','zvalue2','zvalue3','balabala', ""]
    var zadd_scores=[8.4,2,3.3,5,9]
    var count=0
    for (var i=0 ;i<zadd_values.length;++i)
    {
      tair.zadd(zkey,nm,zadd_values[i],zadd_scores[i],function(err, data){ 
          should.not.exist(err);
          data.should.equal(true);
          ++count;
          if(count==zadd_values.length)
          {
            done()
          }
        })
    }

  },0,0);

  it("1.zrangebyscore should work",function(done){
      var zkey="zadd.test.key";
      tair.zrangebyscore(zkey,nm,1,9,function(err,data){
          should.not.exist(err)
          data.length.should.equal(5);
          data[4].should.equal("")
          done();
        })
    })

  it("2.zrangebyscore should work",function(done){
      var zkey="zadd.test.key";
      tair.zrangebyscore(zkey,nm,3,5,function(err,data){
          should.not.exist(err)
          data.length.should.equal(2)
          data[0].should.equal('zvalue3')
          data[1].should.equal('balabala')
          done();
        })
    })
  it("zadd int should work",function(done){
    var zkey="zadd.test.key2";
    var zadd_values=[1,111,11111,1234,0]
    var zadd_scores=[8.4,2,3.3,5,9]
    var count=0
    for (var i=0 ;i<zadd_values.length;++i)
    {
      tair.zadd(zkey,nm,zadd_values[i],zadd_scores[i],function(err, data){ 
          should.not.exist(err);
          data.should.equal(true);
          ++count;
          if(count==zadd_values.length)
          {
            done()
          }
        })
    }

  },0,0);

  it("zadd string of int should work",function(done){
    var zkey="zadd.test.key3";
    var zadd_values=["9223372036854775808","-9223372036854775807","9223372036854775807","45678"]
    var zadd_scores=[1,2,4,5]
    var count=0
    for (var i=0 ;i<zadd_values.length;++i)
    {
      tair.zadd(zkey,nm,zadd_values[i],zadd_scores[i],function(err, data){ 
          should.not.exist(err);
          data.should.equal(true);
          ++count;
          if(count==zadd_values.length)
          {
            tair.zrangebyscore(zkey,nm,1,5,function(err,data){
              should.not.exist(err)
              data.length.should.equal(4)
              data.indexOf(zadd_values[0]).should.not.equal(-1)
              done()
            })
          }
        })
    }

  },0,0);


  it("zadd string of int then zrange should work",function(done){
    var zkey="zadd.test.key3";
    var zadd_values=["9223372036854775808","-9223372036854775807","9223372036854775807","45678"]
    tair.zrange(zkey,nm,0,4,function(err,data){
        should.not.exist(err)
        data.length.should.equal(4)
        data.indexOf(zadd_values[0]).should.not.equal(-1)
        done()
      })

  })
  it("3.zrangebyscore should work",function(done){
      var zkey="zadd.test.key2";
      tair.zrangebyscore(zkey,nm,1,9,function(err,data){
          should.not.exist(err)
          data.length.should.equal(5);
          data[4].should.equal(0);
          done();
        },datatype='int')
    })
  it("4.zrangebyscore should work",function(done){
      var zkey="zadd.test.key2";
      tair.zrangebyscore(zkey,nm,3,5,function(err,data){
          should.not.exist(err)
          data.length.should.equal(2)
          data[0].should.equal(11111)
          data[1].should.equal(1234)
          done();
        },datatype='int')
    })
  it("5.zrangebyscore with a none existed key, should not work",function(done){
      var zkey="zadd.test.key.none";
      tair.zrangebyscore(zkey,nm,1,9,function(err,data){
          err.should.equal(-3998)
          should.not.exist(data)
          done();
        })
    })
  
  it("1.zrange should work",function(done){
      var zkey="zadd.test.key2";
      tair.zrange(zkey,nm,1,3,function(err,data){
          console.log("data="+JSON.stringify(data))
          should.not.exist(err)
          data.length.should.equal(3)
          data[0].should.equal(11111)
          data[1].should.equal(1234)
          data[2].should.equal(1)
          done();
        },datatype='int')
    })
  it("sadd int should work",function(done){
    var skey="sadd.int.test.key";
    var sadd_values=[14,111,11111,1234,0]
    var count=0
    for (var i=0 ;i<sadd_values.length;++i)
    {
      tair.sadd(skey,nm,sadd_values[i],function(err, data){ 
          should.not.exist(err);
          data.should.equal(true);
          ++count;
          if(count==sadd_values.length)
          {
            done()
          }
        })
    }

  },0,0);

  it("smembers should work",function(done){
      var skey="sadd.int.test.key";
      tair.smembers(skey,nm,function(err,data){
          should.not.exist(err)
          data.length.should.equal(5)
          data.indexOf(14).should.above(-1)
          data.indexOf(111).should.above(-1)
          data.indexOf(11111).should.above(-1)
          data.indexOf(1234).should.above(-1)
          data.indexOf(0).should.above(-1)
          done();
        },datatype='int')
    })
  it("sadd string should work",function(done){
    var skey="sadd.string.test.key";
    var sadd_values=["","asdf","123fdg","0","!$@$^"]
    var count=0
    for (var i=0 ;i<sadd_values.length;++i)
    {
      tair.sadd(skey,nm,sadd_values[i],function(err, data){ 
          should.not.exist(err);
          data.should.equal(true);
          ++count;
          if(count==sadd_values.length)
          {
            done()
          }
        })
    }

  },0,0);

  it("smembers of strings should work",function(done){
      var skey="sadd.string.test.key";
      tair.smembers(skey,nm,function(err,data){
          should.not.exist(err)
          data.length.should.equal(5)
          data.indexOf("").should.above(-1)
          data.indexOf("asdf").should.above(-1)
          data.indexOf("123fdg").should.above(-1)
          data.indexOf("0").should.above(-1)
          data.indexOf("!$@$^").should.above(-1)
          done();
        },datatype='string')
    })
 
  it("srem should work",function(done){
      var skey="sadd.int.test.key";
      tair.srem(skey,nm,14,function(err,success){
          should.not.exist(err)
          success.should.equal(true)
          tair.smembers(skey,nm,function(err,data){
              console.log("data="+JSON.stringify(data))
              should.not.exist(err)
              data.length.should.equal(4)
              data.indexOf(111).should.above(-1)
              data.indexOf(11111).should.above(-1)
              data.indexOf(1234).should.above(-1)
              data.indexOf(0).should.above(-1)
              done();
            },datatype='int')
        })
    })
  it("zrem should work",function(done){
      var key="zadd.test.key";
      tair.zrem(key,nm,"zvalue2",function(err,success){
          should.not.exist(err)
          success.should.equal(true)
          tair.zrangebyscore(key,nm,0,1000,function(err,data){
              console.log("data="+JSON.stringify(data))
              should.not.exist(err)
              data.length.should.equal(4)
              data.indexOf("zvalue1").should.above(-1)
              data.indexOf("zvalue3").should.above(-1)
              data.indexOf("balabala").should.above(-1)
              data.indexOf("").should.above(-1)
              done();
            })
        })
    })

  it("zcard should work",function(done){
      var key="zadd.test.key";
      tair.zcard(key,nm,function(err,data){
          should.not.exist(err)
          data.should.equal(4)
          done()
        })
    })
  it("zrem empty string should work",function(done){
      var key="zadd.test.key";
      tair.zrem(key,nm,"",function(err,success){
          should.not.exist(err)
          success.should.equal(true)
          tair.zrangebyscore(key,nm,0,1000,function(err,data){
              console.log("data="+JSON.stringify(data))
              should.not.exist(err)
              data.length.should.equal(3)
              data.indexOf("zvalue1").should.above(-1)
              data.indexOf("zvalue3").should.above(-1)
              data.indexOf("balabala").should.above(-1)
              done();
            })
        })
    })


  it("scard should work",function(done){
      var key="sadd.int.test.key";
      tair.scard(key,nm,function(err,data){
          should.not.exist(err)
          data.should.equal(4)
          done()
        })
    })

  it("hset should work",function(done){
      var key="hset.test.key";
      var fields=['field1','field2','field3','balabala',"","field1"]
      var values=['','value2','value3','balabala', "value5","new+one"]
      var count=0
      for (var i=0 ;i<fields.length;++i)
      {
        tair.hset(key,nm,fields[i],values[i],function(err, data){ 
            should.not.exist(err);
            data.should.equal(true);
            ++count;
            if(count==values.length)
            {
              done()
            }
          })
      }
    })

  it("hgetall should work",function(done){
      var key="hset.test.key";
      var fields=['field1','field2','field3','balabala',"","field1"]
      var values=['','value2','value3','balabala', "value5","new+one"]
      var count=0

      tair.hgetall(key,nm,function(err,data){
        should.not.exist(err)
        data.length.should.equal(5)
        for(obj in data)
        {
          fields.indexOf(obj.field).should.above(-1)
          if(obj.field==='field1')
          {
            obj.value.should.equal("new+one")
          }
          else
          {
            obj.value.should.equal(values[fields.indexOf(obj.field)])
          }
        }
        done()
      })
    })

  it("clear up remove should work",function(done){
      var key_list=['xiang','zadd.test.key','zadd.test.key2','zadd.test.key3','sadd.int.test.key','alargeData',
        "sadd.string.test.key","hset.test.key" ];
      var count=0
      for(var i=0;i<key_list.length;i++){
        console.log('remove '+key_list[i])
        tair.remove(key_list[i],nm,function(err){
          should.not.exist(err)
          count++;
          if(count==key_list.length){
             done();
          }
        })
      }
    })

});
