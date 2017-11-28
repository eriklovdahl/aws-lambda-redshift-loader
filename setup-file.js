/*
    Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

/**
 * Use config file and setup the DynamoDB table entry for the configuration
 */
var pjson = require('./package.json');
var aws = require('aws-sdk');
require('./constants');
var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');
var dynamoDB;
var s3;
var lambda;
var kmsCrypto = require('./kmsCrypto');
var setRegion;

var configJson = process.argv[2] || './config.json';
var setupConfig = require(configJson);

// fake rl for common.js dependency
var rl = {
  close: function () {
    // fake close function
  }
};

var qs = [];

q_config = function (config, callback) {
  callback(null, config, {
    TableName: configTable,
    Item: {
      currentBatch: {
        S: uuid.v4()
      },
      version: {
        S: pjson.version
      },
      loadClusters: {
        L: [{
          M: {}
        }]
      }
    }
  });
};

q_region = function (config, dynamoConfig, callback) {
  var regionsArray = ["ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2", "eu-central-1", "eu-west-1", "sa-east-1", "us-east-1", "us-west-1", "us-west-2"];
  // region for the configuration
  if (common.blank(config.region) !== null) {
    common.validateArrayContains(regionsArray, config.region.toLowerCase(), rl);

    setRegion = config.region.toLowerCase();

    // configure dynamo db and kms for the correct region
    dynamoDB = new aws.DynamoDB({
      apiVersion: '2012-08-10',
      region: setRegion
    });
    kmsCrypto.setRegion(setRegion);
    s3 = new aws.S3({
      apiVersion: '2006-03-01'
    });
    lambda = new aws.Lambda({
      apiVersion: '2015-03-31',
      region: setRegion
    });

    callback(null, config, dynamoConfig);
  } else {
    console.log('You must provide a region from ' + regionsArray.toString())
  }
};

q_s3Prefix = function (config, dynamoConfig, callback) {
  // the S3 Bucket & Prefix to watch for files
  common.validateNotNull(config.s3Prefix, 'You Must Provide an S3 Bucket Name, and optionally a Prefix', rl);

  // setup prefix to be * if one was not provided
  var stripped = config.s3Prefix.replace(new RegExp('s3://', 'g'), '');
  var elements = stripped.split("/");
  var setPrefix = undefined;

  if (elements.length === 1) {
    // bucket only so use "bucket" alone
    setPrefix = elements[0];
  } else {
    // right trim "/"
    setPrefix = stripped.replace(/\/$/, '');
  }

  dynamoConfig.Item.s3Prefix = {
    S: setPrefix
  };

  callback(null, config, dynamoConfig);
};

q_filenameFilter = function (config, dynamoConfig, callback) {
  // a Filename Filter Regex
  if (common.blank(config.filenameFilter) !== null) {
    dynamoConfig.Item.filenameFilterRegex = {
      S: config.filenameFilter
    };
  }
  callback(null, config, dynamoConfig);
};

q_clusterEndpoint = function (config, dynamoConfig, callback) {
  // the Cluster Endpoint
  common.validateNotNull(config.clusterEndpoint, 'You Must Provide a Cluster Endpoint', rl);
  dynamoConfig.Item.loadClusters.L[0].M.clusterEndpoint = {
    S: config.clusterEndpoint
  };
  callback(null, config, dynamoConfig);
};

q_clusterPort = function (config, dynamoConfig, callback) {
  // the Cluster Port
  dynamoConfig.Item.loadClusters.L[0].M.clusterPort = {
    N: '' + common.getIntValue(config.clusterPort, rl)
  };
  callback(null, config, dynamoConfig);
};

q_clusterUseSSL = function (config, dynamoConfig, callback) {
  // Does your cluster use SSL (Y/N)
  dynamoConfig.Item.loadClusters.L[0].M.useSSL = {
    BOOL: common.getBooleanValue(config.clusterUseSSL)
  };
  callback(null, config, dynamoConfig);
};

q_clusterDB = function (config, dynamoConfig, callback) {
  // the Database Name
  if (common.blank(config.clusterDB) !== null) {
    dynamoConfig.Item.loadClusters.L[0].M.clusterDB = {
      S: config.clusterDB
    };
  }
  callback(null, config, dynamoConfig);
};

q_userName = function (config, dynamoConfig, callback) {
  // the Database Username
  common.validateNotNull(config.userName, 'You Must Provide a Username', rl);
  dynamoConfig.Item.loadClusters.L[0].M.connectUser = {
    S: config.userName
  };
  callback(null, config, dynamoConfig);
};

q_userPwd = function (config, dynamoConfig, callback) {
  // the Database Password
  common.validateNotNull(config.userPwd, 'You Must Provide a Password', rl);

  kmsCrypto.encrypt(config.userPwd, function (err, ciphertext) {
    if (err) {
      console.log(JSON.stringify(err));
      process.exit(ERROR);
    } else {
      dynamoConfig.Item.loadClusters.L[0].M.connectPassword = {
        S: kmsCrypto.toLambdaStringFormat(ciphertext)
      };
      callback(null, config, dynamoConfig);
    }
  });
};

q_table = function (config, dynamoConfig, callback) {
  // the Table to be Loaded
  common.validateNotNull(config.table, 'You Must Provide a Table Name', rl);
  dynamoConfig.Item.loadClusters.L[0].M.targetTable = {
    S: config.table
  };
  callback(null, config, dynamoConfig);
};

q_columnList = function (config, dynamoConfig, callback) {
  // the comma-delimited column list (optional)
  if (config.columnList && common.blank(config.columnList) !== null) {
    dynamoConfig.Item.loadClusters.L[0].M.columnList = {
      S: config.columnList
    };
    callback(null, config, dynamoConfig);
  } else {
    callback(null, config, dynamoConfig);
  }
};

q_truncateTable = function (config, dynamoConfig, callback) {
  // Should the Table be Truncated before Load? (Y/N)
  dynamoConfig.Item.loadClusters.L[0].M.truncateTarget = {
    BOOL: common.getBooleanValue(config.truncateTable)
  };
  callback(null, config, dynamoConfig);
};

q_df = function (config, dynamoConfig, callback) {
  // the Data Format (CSV, JSON or AVRO)
  common.validateArrayContains(['CSV', 'JSON', 'AVRO'], config.df.toUpperCase(), rl);
  dynamoConfig.Item.dataFormat = {
    S: config.df.toUpperCase()
  };
  callback(null, config, dynamoConfig);
};

q_csvDelimiter = function (config, dynamoConfig, callback) {
  if (dynamoConfig.Item.dataFormat.S === 'CSV') {
    // the CSV Delimiter
    common.validateNotNull(config.csvDelimiter, 'You Must the Delimiter for CSV Input', rl);
    dynamoConfig.Item.csvDelimiter = {
      S: config.csvDelimiter
    };
    callback(null, config, dynamoConfig);
  } else {
    callback(null, config, dynamoConfig);
  }
};

q_jsonPaths = function (config, dynamoConfig, callback) {
  if (dynamoConfig.Item.dataFormat.S === 'JSON' || dynamoConfig.Item.dataFormat.S === 'AVRO') {
    // the JSON Paths File Location on S3 (or NULL for Auto)
    if (common.blank(config.jsonPaths) !== null) {
      dynamoConfig.Item.jsonPath = {
        S: config.jsonPaths
      };
    }
    callback(null, config, dynamoConfig);
  } else {
    callback(null, config, dynamoConfig);
  }
};

q_manifestBucket = function (config, dynamoConfig, callback) {
  // the S3 Bucket for Redshift COPY Manifests
  common.validateNotNull(config.manifestBucket, 'You Must Provide a Bucket Name for Manifest File Storage', rl);
  dynamoConfig.Item.manifestBucket = {
    S: config.manifestBucket
  };
  callback(null, config, dynamoConfig);
};

q_manifestPrefix = function (config, dynamoConfig, callback) {
  // the Prefix for Redshift COPY Manifests
  common.validateNotNull(config.manifestPrefix, 'You Must Provide a Prefix for Manifests', rl);
  dynamoConfig.Item.manifestKey = {
    S: config.manifestPrefix
  };
  callback(null, config, dynamoConfig);
};

q_failedManifestPrefix = function (config, dynamoConfig, callback) {
  // the Prefix to use for Failed Load Manifest Storage
  common.validateNotNull(config.failedManifestPrefix, 'You Must Provide a Prefix for Manifests', rl);
  dynamoConfig.Item.failedManifestKey = {
    S: config.failedManifestPrefix
  };
  callback(null, config, dynamoConfig);
};

q_accessKey = function (config, dynamoConfig, callback) {
  // the Access Key used by Redshift to get data from S3.
  // If NULL then Lambda execution role credentials will be used
  if (!config.accessKey) {
    callback(null, config, dynamoConfig);
  } else {
    dynamoConfig.Item.accessKeyForS3 = {
      S: config.accessKey
    };
    callback(null, config, dynamoConfig);
  }
};

q_secretKey = function (config, dynamoConfig, callback) {
  // the Secret Key used by Redshift to get data from S3.
  // If NULL then Lambda execution role credentials will be used
  if (!config.secretKey) {
    callback(null, config, dynamoConfig);
  } else {
    kmsCrypto.encrypt(config.secretKey, function (err, ciphertext) {
      if (err) {
        console.log(JSON.stringify(err));
        process.exit(ERROR);
      } else {
        dynamoConfig.Item.secretKeyForS3 = {
          S: kmsCrypto.toLambdaStringFormat(ciphertext)
        };
        callback(null, config, dynamoConfig);
      }
    });
  }
};

q_symmetricKey = function (config, dynamoConfig, callback) {
  // If Encrypted Files are used, Enter the Symmetric Master Key Value
  if (config.symmetricKey && common.blank(config.symmetricKey) !== null) {
    kmsCrypto.encrypt(config.symmetricKey, function (err, ciphertext) {
      if (err) {
        console.log(JSON.stringify(err));
        process.exit(ERROR);
      } else {
        dynamoConfig.Item.masterSymmetricKey = {
          S: kmsCrypto.toLambdaStringFormat(ciphertext)
        };
        callback(null, config, dynamoConfig);
      }
    });
  } else {
    callback(null, config, dynamoConfig);
  }
};

q_failureTopic = function (config, dynamoConfig, callback) {
  // the SNS Topic ARN for Failed Loads
  if (common.blank(config.failureTopic) !== null) {
    dynamoConfig.Item.failureTopicARN = {
      S: config.failureTopic
    };
  }
  callback(null, config, dynamoConfig);
};

q_successTopic = function (config, dynamoConfig, callback) {
  // the SNS Topic ARN for Successful Loads
  if (common.blank(config.successTopic) !== null) {
    dynamoConfig.Item.successTopicARN = {
      S: config.successTopic
    };
  }
  callback(null, config, dynamoConfig);
};

q_batchSize = function (config, dynamoConfig, callback) {
  // How many files should be buffered before loading?
  if (common.blank(config.batchSize) !== null) {
    dynamoConfig.Item.batchSize = {
      N: '' + common.getIntValue(config.batchSize, rl)
    };
  }
  callback(null, config, dynamoConfig);
};

q_batchBytes = function (config, dynamoConfig, callback) {
  // Batches can be buffered up to a specified size. How large should a batch
  // be before processing (bytes)?
  if (common.blank(config.batchSizeBytes) !== null) {
    dynamoConfig.Item.batchSizeBytes = {
      N: '' + common.getIntValue(config.batchSizeBytes, rl)
    };
  }
  callback(null, config, dynamoConfig);
};

q_batchTimeoutSecs = function (config, dynamoConfig, callback) {
  // How old should we allow a Batch to be before loading (seconds)?
  if (common.blank(config.batchTimeoutSecs) !== null) {
    dynamoConfig.Item.batchTimeoutSecs = {
      N: '' + common.getIntValue(config.batchTimeoutSecs, rl)
    };
  }
  callback(null, config, dynamoConfig);
};

q_copyOptions = function (config, dynamoConfig, callback) {
  // Additional Copy Options to be added
  if (common.blank(config.copyOptions) !== null) {
    dynamoConfig.Item.copyOptions = {
      S: config.copyOptions
    };
  }
  callback(null, config, dynamoConfig);
};

last = function (config, dynamoConfig, callback) {
  rl.close();
  setup(dynamoConfig, callback);
};

setup = function (dynamoConfig, callback) {
  common.setup(dynamoConfig, dynamoDB, s3, lambda, callback);
};

// export the setup module so that customers can programmatically add new
// configurations
exports.setup = setup;

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_filenameFilter);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_clusterUseSSL);
qs.push(q_clusterDB);
qs.push(q_table);
qs.push(q_columnList);
qs.push(q_truncateTable);
qs.push(q_userName);
qs.push(q_userPwd);
qs.push(q_df);
qs.push(q_csvDelimiter);
qs.push(q_jsonPaths);
qs.push(q_manifestBucket);
qs.push(q_manifestPrefix);
qs.push(q_failedManifestPrefix);
qs.push(q_accessKey);
qs.push(q_secretKey);
qs.push(q_successTopic);
qs.push(q_failureTopic);
qs.push(q_batchSize);
qs.push(q_batchBytes);
qs.push(q_batchTimeoutSecs);
qs.push(q_copyOptions);
qs.push(q_symmetricKey);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
async.someSeries(setupConfig.loaders || [setupConfig], function (loader, callback) {
  var mergedConfig = Object.assign({}, setupConfig, loader);

  console.log('Configuring loader for prefix ' + mergedConfig.s3Prefix + ' into table ' + mergedConfig.table + ' @ ' + mergedConfig.region);
  async.waterfall([async.apply(q_config, mergedConfig)].concat(qs), (err) => {
    callback(null, !err);
  });
}, function (err, result) {
  console.log('Done');
});
