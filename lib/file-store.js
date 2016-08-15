'use strict';
var path = require('path'),
   // fs = require('fs-extra'),
    async = require('async'),
    crypto = require('crypto'),
    mkdirp = require('mkdirp'),
    utils = require('./utils'),
    _ = require('lodash');

var FileStore = function (rootDirectory,fs) {
  var CONTENT_FILE = '.dummys3_content',
      METADATA_FILE = '.dummys3_metadata',
      MULTIPART_META_FILE = '.dummys3_metadata_multipart',
      MULTIPART_FILE = '.dummys3_multipart',
      Bucket = require('./models/bucket'),
      S3Object = require('./models/s3-object');
  if (!fs){
      fs = require('fs-extra');
  }
  var getBucketPath = function (bucketName) {
    return path.join(rootDirectory, bucketName).replace(/\\/g, '/');
  };

  var getBucket = function (bucketName, done) {
    var bucketPath = getBucketPath(bucketName);
    fs.stat(bucketPath, function (err, file) {
      if (err || !file.isDirectory()) {
        return done('Bucket not found');
      }
      return done(null, new Bucket(bucketName, file.ctime));
    });
  };

  var deleteBucket = function (bucket, done) {
    var bucketPath = getBucketPath(bucket.name);
    fs.rmdir(bucketPath, function (err) {
      if (err) {
        return done(err);
      }
      return done();
    });
  };

  var getBuckets = function () {
    var buckets = [];
    fs.readdirSync(rootDirectory).filter(function (result) {
      var file = fs.statSync(path.resolve(rootDirectory, result));
      if (file.isDirectory()) {
        buckets.push(new Bucket(result, file.ctime));
      }
    });
    return buckets;
  };

  var putBucket = function (bucketName, done) {
    var bucketPath = getBucketPath(bucketName);
    fs.mkdir(bucketPath, 502, function (err) {
      if (err) {
        return done(err);
      }
      return getBucket(bucketName, done);
    });
  };

  var getObject = function (bucket, key, range, done) {
    if (typeof range === 'function') {
      done = range;
      range = null;
    }
    var filePath = path.resolve(getBucketPath(bucket.name), key);
    fs.stat(filePath, function (err, stats) {
      if (err && err.code === 'ENOENT') {
        return done('Not found');
      }
      var options = {};
      if (range) {
        var positions = range.replace(/bytes=/, '').split('-');
        options.start = parseInt(positions[0], 10);
        if (positions[1]) options.end = parseInt(positions[1], 10);
      }
      async.parallel([
        function (callback) {
          var readStream = fs.createReadStream(path.join(filePath, CONTENT_FILE), options);
          readStream.range = range && options;
          readStream.on('error', function (err) {
            return callback(err);
          });
          readStream.on('open', function () {
            return callback(null, readStream);
          });
        },
        function (callback) {
          fs.readFile(path.join(filePath, METADATA_FILE), function (err, data) {
            if (err) {
              return callback(err);
            }
            callback(null, buildS3ObjectFromMetaDataFile(key, data));
          });
        }
      ], function (err, results) {
        if (err) {

          return done(err);
        }
        return done(null, results[1], results[0]);
      });
    });
  };

  var getObjects = function (bucket, options, done) {
    var bucketPath = getBucketPath(bucket.name);
    var objects = [];
    var commonPrefixes = [];
    var keys = utils.walk(bucketPath);

    if (keys.length === 0) {
      return done(null, {
        objects: [],
        commonPrefixes: []
      });
    }

    var filteredKeys = [];

    if (options.delimiter && options.prefix) {
      _.forEach(keys, function (key) {
        var truncatedKey = key.replace(bucketPath + "/", "");
        if (truncatedKey.slice(0, options.prefix.length) == options.prefix) {
          if (truncatedKey.indexOf(options.delimiter, options.prefix.length + 1) > -1) {
            var commonPrefix = truncatedKey.substring(0, truncatedKey.lastIndexOf(options.delimiter) + 1);
            if (commonPrefixes.indexOf(commonPrefix) == -1) {
              commonPrefixes.push(commonPrefix);
              commonPrefixes.sort();
            }
          } else {
            filteredKeys.push(key);
          }
        }
      });
    } else if (options.prefix) {
      _.forEach(keys, function (key) {
        var truncatedKey = key.replace(bucketPath + "/", "");
        if (truncatedKey.slice(0, options.prefix.length) == options.prefix) {
          filteredKeys.push(key);
        }
      });
    } else if (options.delimiter) {
      _.forEach(keys, function (key) {
        var truncatedKey = key.replace(bucketPath + "/", "");
        if (truncatedKey.indexOf(options.delimiter) > -1) {
          var commonPrefix = truncatedKey.substring(0, truncatedKey.indexOf(options.delimiter) + 1);
          if (commonPrefixes.indexOf(commonPrefix) == -1) {
            commonPrefixes.push(commonPrefix);
            commonPrefixes.sort();
          }
        } else {
          filteredKeys.push(key);
        }
      });

    } else {
      filteredKeys = keys;
    }

    filteredKeys.sort();
    if (options.marker) {
      var startAt = 0;
      var found = false;
      _.each(filteredKeys, function (key, index) {
        if (options.marker == key.replace(bucketPath + '/', '')) {
          startAt = index + 1;
          found = true;
        }
      });
      filteredKeys = (found) ? filteredKeys.slice(startAt) : [];
    }

    filteredKeys = filteredKeys.slice(0, options.maxKeys);

    async.eachSeries(filteredKeys, function (key, callback) {
      key = key.replace(/\\/g, '/');
      fs.readFile(path.join(key, METADATA_FILE), function (err, data) {
        if (!err) {
          objects.push(buildS3ObjectFromMetaDataFile(key.replace(bucketPath + '/', ''), data));
        }
        callback();
      });
    }, function () {
      return done(null, {
        objects: objects,
        commonPrefixes: commonPrefixes
      });
    });
  };

  var buildS3ObjectFromMetaDataFile = function (key, file) {
    var json = JSON.parse(file);
    var metaData = {
      key: key,
      md5: json.md5,
      contentType: json.contentType,
      contentEncoding: json.contentEncoding,
      contentDisposition: json.contentDisposition,
      size: json.size,
      modifiedDate: json.modifiedDate,
      creationDate: json.creationDate,
      customMetaData: json.customMetaData
    };
    return new S3Object(metaData);
  };

  var getCustomMetaData = function (headers) {
    var customMetaData = [];
    for (var header in headers) {
      if (/^x-amz-meta-(.*)$/.test(header)) {
        customMetaData.push({
          key: header,
          value: headers[header]
        });
      }
    }
    return customMetaData;
  };


  var createMetaData = function (data, done) {
    var contentFile = data.contentFile,
        type = data.type,
        encoding = data.encoding,
        disposition = data.disposition,
        metaFile = data.metaFile,
        headers = data.headers;
    async.parallel([
      function (callback) {
        fs.stat(contentFile, function (err, stats) {
          if (err) {
            return callback(err);
          }
          return callback(null, {
            mtime: stats.mtime,
            ctime: stats.ctime
          });
        });
      },
      function (callback) {
        var length = 0;
        var md5 = crypto.createHash('md5')
        var stream = fs.createReadStream(contentFile)

        stream.on('error', function (err) {
          return callback(err)
        });

        stream.on('data', function (data) {
          length += data.length;
          md5.update(data, 'utf8');
        });

        stream.on('end', function () {
          return callback(null, {
            size: length,
            md5: md5.digest('hex')
          });
        });
      }
    ], function (err, results) {
      var metaData = {
        md5: results[1].md5,
        contentType: type,
        size: results[1].size,
        modifiedDate: results[0].mtime,
        creationDate: results[0].ctime,
        customMetaData: getCustomMetaData(headers)
      };
      if (encoding)
        metaData.contentEncoding = encoding;
      if (disposition)
        metaData.contentDisposition = disposition;

      fs.writeFile(metaFile, JSON.stringify(metaData), function (err) {
        if (err) {
          return done(err);
        }
        return done(null, metaData);
      });
    });
  };

  var initiateMultipart = function (bucket, key, metadata, done) {
    var keyName = path.join(bucket, key);
    var dirName = path.join(rootDirectory, keyName);
    mkdirp.sync(dirName);
    var metaFile = path.join(dirName, MULTIPART_META_FILE);
    fs.writeFile(metaFile, JSON.stringify(metadata), function (err) {
      done(err);
    });
  };

  var getObjectPart = function (bucket, key, partNumber, done) {
    var rootPath = path.resolve(getBucketPath(bucket), key);
    var filePath = path.join(rootPath, '.' + partNumber + MULTIPART_FILE);
    fs.stat(filePath, function (err, stats) {
      if (err && err.code === 'ENOENT') {
        return done('Not found');
      }
      var options = {};
      var readStream = fs.createReadStream(filePath, options);
      readStream.on('error', function (err) {
        done(err);
      });
      readStream.on('open', function () {
        done(null, stats.size, readStream);
      });
    });
  };
  var deleteObjectPart = function (bucket, key, partNumber, done) {
    var filePath = path.resolve(getBucketPath(bucket), key);
    fs.stat(filePath, function (err, stats) {
      if (err && err.code === 'ENOENT') {
        return done('Not found');
      }
      var fp = path.join(filePath, '.' + partNumber + MULTIPART_FILE);
      var fpMeta = path.join(filePath, '.' + partNumber + METADATA_FILE);
      async.parallel([
        function (callback) {
          fs.unlink(fp, function () {
            callback();
          });
        },
        function (callback) {
          fs.unlink(fpMeta, function () {
            callback();
          });
        }
      ], function () {
        done();
      })
    });
  };
  var getObjectMultipartMeta = function (bucket, key, done) {
    var filePath = path.resolve(getBucketPath(bucket), key);
    fs.stat(filePath, function (err, stats) {
      if (err && err.code === 'ENOENT') {
        return done('Not found');
      }
      fs.readFile(path.join(filePath, MULTIPART_META_FILE), function (err, data) {
        if (err) {
          return done(err);
        }
        done(null, JSON.parse(data));
      });
    });
  };

  var putMultipartPart = function (bucket, key, partNumber, req, done) {
    var keyName = path.join(bucket, key);
    var dirName = path.join(rootDirectory, keyName);
    var fileName = path.join(dirName, '.' + partNumber + MULTIPART_FILE);
    var metaFile = path.join(dirName, '.' + partNumber + METADATA_FILE);
    var writeStream = req.pipe(fs.createWriteStream(fileName));
    writeStream.on('error', function (err) {
      return done('Error writing file');
    });
    writeStream.on('finish', function () {
      writeStream.end();
      createMetaData({
        contentFile: fileName,
        type: req.headers['content-type'],
        encoding: req.headers['content-encoding'],
        disposition: req.headers['content-disposition'],
        key: key,
        metaFile: metaFile,
        headers: req.headers
      }, function (err, metaData) {
        if (err) {
          return done('Error uploading file');
        }
        return done(null, new S3Object(metaData));
      });
    });
  };

  var putObject = function (bucket, req, done) {
    var keyName = path.join(bucket.name, req.params.key);
    var dirName = path.join(rootDirectory, keyName);
    mkdirp.sync(dirName);
    if (req.query.uploadId && req.query.partNumber) {
      putMultipartPart(bucket.name, req.params.key, req.query.partNumber, req, done);
      return;
    }
    var contentFile = path.join(dirName, CONTENT_FILE);
    var metaFile = path.join(dirName, METADATA_FILE);
    var key = req.params.key;
    key = key.substr(key.lastIndexOf('/') + 1);
    var writeStream = req.pipe(fs.createWriteStream(contentFile));
    writeStream.on('error', function (err) {
      return done('Error writing file');
    });
    writeStream.on('finish', function () {
      writeStream.end();
      createMetaData({
        contentFile: contentFile,
        type: req.headers['content-type'],
        encoding: req.headers['content-encoding'],
        disposition: req.headers['content-disposition'],
        key: key,
        metaFile: metaFile,
        headers: req.headers
      }, function (err, metaData) {
        if (err) {
          return done('Error uploading file');
        }
        return done(null, new S3Object(metaData));
      });
    });
  };

  var copyObject = function (options, done) {

    var req = options.request,
        srcBucket = options.srcBucket,
        srcKey = options.srcKey,
        destKey = options.destKey,
        destBucket = options.destBucket,
        srcKeyPath = path.resolve(getBucketPath(srcBucket.name), srcKey),
        destKeyPath = path.resolve(getBucketPath(destBucket.name), destKey),
        srcMetadataFilePath = path.join(srcKeyPath, METADATA_FILE),
        srcContentFilePath = path.join(srcKeyPath, CONTENT_FILE),
        destMetadataFilePath = path.join(destKeyPath, METADATA_FILE),
        destContentFilePath = path.join(destKeyPath, CONTENT_FILE);

    if (srcKeyPath == destKeyPath) {
      createMetaData({
        contentFile: srcContentFilePath,
        type: req.headers['content-type'],
        encoding: req.headers['content-encoding'],
        key: srcKey,
        metaFile: srcMetadataFilePath,
        headers: req.headers
      }, function (err, metaData) {
        if (err) {
          return done('Error updating metadata');
        }
        return done(null, new S3Object(metaData));
      });
    } else {
      mkdirp.sync(destKeyPath);
      fs.copySync(srcMetadataFilePath, destMetadataFilePath);
      fs.copySync(srcContentFilePath, destContentFilePath);

      fs.readFile(destMetadataFilePath, function (err, data) {
        if (err) {
          return done(err);
        }
        done(null, buildS3ObjectFromMetaDataFile(destKey, data));
      });
    }
  };

  var deleteObject = function (bucket, key, done) {
    var keyPath = path.resolve(getBucketPath(bucket.name), key);
    async.map([path.join(keyPath, METADATA_FILE),
      path.join(keyPath, CONTENT_FILE)], fs.unlink, function (err) {
      if (err) {
        return done(err);
      }
      fs.rmdir(keyPath, function () {
        return done();
      });
    });
  };

  var getObjectExists = function (bucket, key, done) {
    var keyPath = path.resolve(getBucketPath(bucket.name), key);
    fs.stat(keyPath, function (err, file) {
      if (err || !file.isDirectory()) {
        return done('Object not found for ' + keyPath);
      }
      return done(null);
    });
  };

  return {
    getBuckets: getBuckets,
    getBucket: getBucket,
    putBucket: putBucket,
    deleteBucket: deleteBucket,
    getObjects: getObjects,
    getObject: getObject,
    putObject: putObject,
    copyObject: copyObject,
    getObjectExists: getObjectExists,
    deleteObject: deleteObject,
    initiateMultipart: initiateMultipart,
    getObjectPart: getObjectPart,
    deleteObjectPart: deleteObjectPart,
    getObjectMultipartMeta: getObjectMultipartMeta
  };
};
module.exports = FileStore;
