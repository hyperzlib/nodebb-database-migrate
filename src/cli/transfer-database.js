var nconf = require('nconf');
var async = require('async');

function getDbInterface(databaseName){
    return new Promise((resolve, reject) => {
        var primaryDB = require('../database/' + databaseName);

        primaryDB.parseIntFields = function (data, intFields, requestedFields) {
            intFields.forEach((field) => {
                if (!requestedFields.length || requestedFields.includes(field)) {
                    data[field] = parseInt(data[field], 10) || 0;
                }
            });
        };

        primaryDB.initSessionStore = function (callback) {
            const sessionStoreConfig = nconf.get('session_store') || nconf.get('redis') || nconf.get(databaseName);
            let sessionStoreDB = primaryDB;

            if (nconf.get('session_store')) {
                sessionStoreDB = require('../database/' + sessionStoreConfig.name);
            } else if (nconf.get('redis')) {
                // if redis is specified, use it as session store over others
                sessionStoreDB = require('../database/redis');
            }

            sessionStoreDB.createSessionStore(sessionStoreConfig, function (err, sessionStore) {
                if (err) {
                    return callback(err);
                }
                primaryDB.sessionStore = sessionStore;
                callback();
            });
        };

        primaryDB.keys = function (callback) {
            if(databaseName == 'redis'){
                primaryDB.client.send_command('keys', ['*'], callback);
            } else if(databaseName == 'mongo'){
                primaryDB.client.collection("objects").aggregate(
                    [
                        { 
                            "$project" : {
                                "_key" : "$_key"
                            }
                        }, 
                        { 
                            "$group" : {
                                "_id" : null, 
                                "distinct" : {
                                    "$addToSet" : "$$ROOT"
                                }
                            }
                        }, 
                        { 
                            "$unwind" : {
                                "path" : "$distinct", 
                                "preserveNullAndEmptyArrays" : false
                            }
                        }, 
                        { 
                            "$replaceRoot" : {
                                "newRoot" : "$distinct"
                            }
                        }
                    ], 
                    { "allowDiskUse" : true },
                ).toArray(function (err, result) {
                    if(err){
                        return callback(err);
                    }
                    if(Array.isArray(result)){
                        let keyList = [];
                        result.forEach((one) => {
                            keyList.push(one._key);
                        });
                        callback(err, keyList);
                    } else {
                        callback(null, []);
                    }
                });
            }
        }

        primaryDB.init(() => {
            resolve(primaryDB);
        });
    });
}

function fliteKeys(keys){
    let disallowRegexp = /(sess|nodebbpostsearch|nodebbtopicsearch):.*/g;
    let allow = [];
    keys.forEach((one) => {
        if(one.match(disallowRegexp) == null){
            allow.push(one);
        }
    });
    return allow;
}

function parseType(str){
    if(str === 'true'){
        return true;
    } else if(str === 'false'){
        return false;
    } else if(!!str.match(/^(-|)[0-9]+$/)){
        return parseInt(str);
    } else {
        return str;
    }
}

function ObjectParseType(object){
    let ret = {};
    for(let key in object){
        let one = object[key];
        if(typeof one === 'string'){
            ret[key] = parseType(one);
        } else {
            ret[key] = one;
        }
    }
    return ret;
}

function transDb(next){
    Promise.all([
        getDbInterface(nconf.get('database')),
        getDbInterface(nconf.get('targetDatabase'))
    ]).then((dbi) => {
        let [srcDbi, dstDbi] = dbi;
        let timestamp = Date.now();
        async.waterfall([
            function(callback){
                dstDbi.keys((err, data) => {
                    if(err) return callback(err);
                    
                    let keys = fliteKeys(data);
                    dstDbi.deleteAll(keys, callback);
                });
            },
            function(callback){
                srcDbi.keys((err, data) => {
                    if(err) return callback(err);

                    let keys = fliteKeys(data);
                    let options = [];
                    let types = [];
                    for(let i = 0; i < keys.length; i ++){
                        let key = keys[i];
                        let func = (callback) => {
                            srcDbi.type(key, (err, type) => {
                                if(err) return callback(err);
                                
                                switch(type){
                                    case 'zset':
                                        srcDbi.getSortedSetRange(key, 0, -1, (err, values) => {
                                            if(err) return callback(err);
        
                                            let scores = [];
                                            for(let i = 0; i < values.length; i ++){
                                                scores.push(timestamp - values.length + i);
                                            }
                                            dstDbi.sortedSetAdd(key, scores, values, callback);
                                        });
                                        break;
                                    case 'hash':
                                        srcDbi.getObject(key, (err, values) => {
                                            if(err) return callback(err);

                                            values = ObjectParseType(values);
                                            dstDbi.setObject(key, values, callback);
                                        });
                                        break;
                                    case 'set':
                                        srcDbi.getSetMembers(key, (err, values) => {
                                            if(err) return callback(err);

                                            dstDbi.setAdd(key, values, callback);
                                        });
                                        break;
                                    case 'list':
                                        srcDbi.getListRange(key, 0, -1, (err, values) => {
                                            if(err) return callback(err);
                                            
                                            let listOpts = [];
                                            for(let j = 0; j < values.length; j ++){
                                                let one = values[j];
                                                listOpts.push((listCallback) => {
                                                    dstDbi.listAppend(key, one, listCallback);
                                                });
                                            }
                                            async.waterfall(listOpts, callback);
                                        });
                                        break;
                                    case 'string':
                                        srcDbi.get(key, (err, value) => {
                                            if(err) return callback(err);
                                            
                                            dstDbi.set(key, value, callback);
                                        });
                                        break;
                                    default:
                                        callback();
                                        break;
                                }
                                console.log('Transfered: ' + key);
                            });
                        }
                        options.push(func);
                    }
        
                    async.waterfall(options, (err) => {
                        if(err){
                            return callback(err);
                        }
                        callback();
                    });
                });
            }
        ], (err) => {
            if(err){
                console.log(err);
            }
            next();
        });
    }).catch((err) => {
        console.log(err);
        next();
    });
}

exports.transDb = transDb;