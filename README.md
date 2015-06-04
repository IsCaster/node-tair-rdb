# Node Tair Rdb

Taobao [tair-rdb](http://code.taobao.org/p/tair/src/tags/tair_rdb_A_2_3_5_5_58_20130415/) Client for Node.js, build on pure javascript without any native code. Tair-rdb is a subproject of [tair](http://code.taobao.org/p/tair/src/), using redis storage engine, support all data type of redis.

node-tair-rdb has two parts of APIs. One called node-tair-rdb client, the other called node-tair-rdb model.

node-tair-rdb client APIs was writed based on [node tair](https://github.com/wangbinke/node-tair),  expanded with redis commands

## Installation

	$ npm install tair-rdb (TODO not registered in npm yet.)

Strongly recommand you use node >= 0.8.0 to use this lib for its performance improve on io and buffer.

## An Example of tair-rdb client

````js
var cli = require('tair').client;

var configServer = [{host: '192.168.2.201', port: 5198}]; /* you can also add another slave server to it */
// firstly must
var tair = new cli('group_name', configServer, function (err){
  if (err) {
    console.log(err);
  }
  tair.set('key', 'value', function(err, isSuccess){
    console.log(success);
  });
});
````

## APIs of tair-rdb Client

````js
	Tair(groupName, hostList, callback)
	 * initial clients from config servers, must be first called, all three params must be used
	 * @params groupnName：group name of tair
	 * @params hostList: config server list of tair, like [{host: '10.235.144.116', port: 5198}]
	 * @params options
        * heartBeatInterval = 10 * 1000 {Number} interval time for heartbeat, mili-seconds
        * timeout = 5000 {Number}, timeout for network, mili-seconds
	 * @params callback(err)


	Tair.set / Tair.setEx (key, value, [expire], [namespace], [version], callback)
	 * set a key with a value
	 * @params key：must be string, the key to set
	 * @params value: usually string, the value to set
	 * @params expire: seconds to expire, number, optional, default is 0 (not expired)
	 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
	 * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0
	 * @params callback(err, success): success is true when set successfully


	Tair.get (key, [namespace], callback, fitJava, dataType)
	 * get a key from a datanode
     * @params key：must be string, the key to get
     * @params namespace: the area(namespace) of data, number 0~1023.
     * @params callback(err, data):
     * @params fitJava: true/ false, set to fit java key type
     * @params datatype: support string/buffer/float/double/int , returned data type, default is string.

	Tair.remove (key, [namespace], callback)
	 * remove / delete a key from a datanode
	 * @params key：must be string, the key to remove
	 * @params namespace: the area(namespace) of data, number 0~1023
	 * @params callback(err)


	Tair.incr / Tair.decr (key, [count], [namespace], [initValue], [expire], callback)
	 * increace or decreace a count object on tair, count object is different from object from usually set / get. First use these method on a key will create it with initValue or 0;
	 * @params key：must be string
	 * @params count: amount to plus or minus, usually be positive number
	 * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
	 * @params initValue: if key is not exist, give it a value
	 * @params expire: if key is not exist,set the expire(seconds) of new value 
	 * @params callback(err, data): data is the count number after incr or decr

    Tair.smembers (key, namespace, callback, datatype)
     * get all members of a set 
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
     * @params callback(err, data):
     * @params datatype: 'string' or 'buffer', returned data type, default is string

    Tair.zrangebyscore (key, namespace, start, end, callback, datatype, limit, with_score )
     * Returns all the elements in the sorted set at `key` with a score between `start` and `end` (including elements with score equal to `start` or `end`). The elements are considered to be ordered from low to high scores.
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, must set here
     * @params start: start of the score, double type, 8 bytes
     * @params end: end of the score, double type, 8 bytes
     * @params callback(err, data):
     * @params datatype: 'string' or 'buffer' or ..., returned data type, default is string.
     * @params limit: max length of result, TODO
     * @params with_score: whether or not return score, TODO     

    Tair.zrange (key, namespace, start, end, callback, datatype, limit, with_score ) 
     * Returns all the elements in the sorted set at `key` with index between `start` and `end` (including elements with index equal to `start` or `end`). The elements are considered to be ordered from low to high scores.
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, must set here
     * @params start: start index, int type, 4 bytes
     * @params end: end index, int type, 4 bytes
     * @params callback(err, data):
     * @params datatype: 'string' or 'buffer' or..., returned data type, default is string.
     * @params with_score: whether or not return score, TODO

    Tair.zadd (key, namespace, value, score, callback, expire, version) 
     * Add a `value` with `score` to the sorted set stored at `key`
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, must set here
     * @params value: usually string, the value to set
     * @params score: double typ, 8 bytes
     * @params callback(err, data)
     * @params expire: seconds to expire, number, optional, default is 0 (not expired)
     * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0 

    Tair.sadd (key, namespace, value, callback, expire, version)
     * Add a `value` to the set stored at `key`
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, must set here
     * @params value: usually string, the value to set
     * @params callback(err, data)
     * @params expire: seconds to expire, number, optional, default is 0 (not expired)
     * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0

    Tair.srem (key, namespace, value, callback, expire, version)
     * remove a `value` of the set stored at `key`
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, must set here
     * @params value: usually string, the value to remove
     * @params callback(err, data)
     * @params expire: seconds to expire, number, optional, default is 0 (not expired)
     * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0

    Tair.zrem (key, namespace, value, callback, expire, version)
     * remove a `value` of the sorted set stored at `key`
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, must set here
     * @params value: usually string, the value to remove
     * @params callback(err, data)
     * @params expire: seconds to expire, number, optional, default is 0 (not expired)
     * @params version: the version of data, using to solve concurrency conflicts, not commonly used in cache, optional, default is 0

    Tair.scard (key, namespace, callback)
     * Returns the set cardinality (number of elements) of the set stored at key
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
     * @params callback(err, data):

    Tair.zcard (key, namespace, callback)
     * Returns the set cardinality (number of elements) of the sorted set stored at key
     * @params key：must be string
     * @params namespace: the area(namespace) of data, number 0~1023, optional, default is 0
     * @params callback(err, data):
````

## An Example of tair-rdb model
````js
    //First, we need a json data to define what database table you want to create.
    //if it's a single table ,set `multi-table` to false ,else set it to true, default is false
    var ad_space_model_conf={
        "name":"ad.space",
        "multi-table":true;
        "attr":[
            {
                "key":"desc",
                "type":"string",
                "default":"",    
            },
            {
                "key":"type",
                "type":"int",
                "default":1,    
            },
            {
                "key":"ad.group.set",
                "type":"zset",
                "default":{},    
            },
            {
                "key":"width",
                "type":"int",
                "default":40,    
            },
            {
                "key":"height",
                "type":"int",
                "default":30,    
            },
        ]}
    //Then, we need create db connection.
    var db = require("tair-rdb").db
    var conn = new db('group_1',[
            {host: '192.168.2.201', port: 5198}
        ],
        {heartBeatInterval: 3000},
        function(err)
        {
            if (err) {
                console.log("database connection error:"+err)
            }
        }
    )
    //create table ad.space class .
    var model=require("tair-rdb").model
    model.load(this,ad_space_model_conf,db)
    //Now you can use class ad_space, to generate table class
    var mall_id=447
    var ad_space_tbl=new ad_space(mall_id)
    var new_ad_space_entry=new ad_space_tbl()
    new_ad_space_entry.desc='app portal page'
    new_ad_space_entry.ad_group_set["123456"]=123456
    new_ad_space_entry.ad_group_set["123457"]=123457
    //using the default value of other field, save the entry to database
    new_ad_space_entry.save().then()
    //then function accept a function parameter to run after save()
````
If it's not a multi-table, then ad_space is the table class
````js    
    var new_ad_space_entry=new ad_space()    
    new_ad_space_entry.desc='app banner ad'
    new_ad_space_entry.ad_group_set["123458"]=123458
    new_ad_space_entry.ad_group_set["123459"]=123459
    new_ad_space_entry.save()
````
if the table name or attribute name is a invalid identifier ,the illegal chars would be replaced by "_",
for example:
- ad.group.set => ad_group_set
- ad.space => ad_space
````js
    console.log("new_ad_space_entry is "+JSON.stringify(new_ad_space_entry))
    //new_ad_space_entry will get a property 'id' when insert it into database, assume the id is 1024

    //Query the table
    //get the entry we just insert into
    ad_space_tbl.objects.get(1024).then(function(the_ad_space_entry){...})
    //the_ad_space_entry is a object of class ad_space_tbl
````
Get array of objects of class ad_space_tbl , which type is 1 and desc contains app
````js
    ad_space_tbl.objects.filter({"desc__contains":"app","type":1}).then(function(the_ad_space_entries){})
````
get all data of table ad_space_tbl with mall_id=447
````js    
    ad_space_tbl.objects.all().then(function(all_data_of_ad_space){ } )
````
change data of the first entry in the_ad_space_entries
````js
    the_ad_space_entries[0].desc="app the other banner ad"
    the_ad_space_entries[0].width=60
    the_ad_space_entries[0].height=80
    the_ad_space_entries[0].save()
````
delete the second entry in the the_ad_space_entries
````js
    the_ad_space_entries[1].remove().then()
````
remove action is executed by record id ,if the record is not saved then the remove action would fail.
all the database operation is run asynchronous, so mostly you need put you code after database
operations into then function 

### Infomation and Caution

0. Tair use config server to store the config of all datanode in a cluster. The client firstly request config server to get the bucket / copy / datanode infomation. And use [MurMurHash2](http://en.wikipedia.org/wiki/MurmurHash) Algorithm to decide a key to store on which datanode. Nearly all load balancing work is done on client-side. So when client is out of sync with config servers, the service maybe down.

0. Tair use area(namespace) like database in traditional relation databases, number 0~1023, but  strange bugs occurs using area 0 . 
so please **DON'T USE AREA 0**

0. Javascript only have `number` type, so we treat `number` with decimal as datatype `double`, without decimal as datatype `int`. We Treat datatype `float` as the same of `double` using 8 bytes. You'd better use `buffer` type when you try to save `float` type data or you can use tair-rdb model APIs directly.

0. all expire parameters is not tested.


