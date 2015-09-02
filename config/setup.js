var pjson = require('../package.json');
var readline = require('readline');
var aws = require('aws-sdk');
require('../constants');
var common = require('../common');
var async = require('async');
var uuid = require('node-uuid');
var dynamoDB;
var kmsCrypto = require('../kmsCrypto');

dynamoConfig = {
	TableName : configTable,
	Item : {
		truncateTarget : {
			BOOL : false
		},
		currentBatch : {
			S : uuid.v4()
		},
		version : {
			S : pjson.version
		},
		loadClusters : {
			L : [ {
				M : {

				}
			} ]
		}
	}
};

// Cluster parameters from connection string
var connectionString = process.env.REDSHIFT_CONNECTION_STRING
var clusterParams = {}
connectionString.split(' ').forEach(function(d){
    d = d.split('=');
    clusterParams[d[0]] = d[1];
})
var clusterEndpoint = clusterParams['host'];
var clusterPort = clusterParams['port'];
var databaseName = clusterParams['dbname'];
var databaseUser = clusterParams['user'];
var databasePassword = clusterParams['password'];

// change this to use environment variables asap
var region = 'us-east-1';
var setRegion = region;
var s3BucketWithPrefix = 'artsy-analytics/segment-logs';
var s3DataFormat = 'JSON';
var jsonPaths =  's3://artsy-analytics/json_paths/raw_segment_events'
var manifestBucket = 'artsy-analytics';
var manifestPrefix = 'copy_manifests';
var failedManifestPrefix = 'failed_load_manifests';
var AWSAccessKey = process.env.AWS_ID
var AWSSecret = process.env.AWS_SECRET
var copyOptions = "GZIP  timeformat 'auto'";
var targetTable = 'segment.raw_events';

var qs = [];

setOptions = function(callback){

  dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : region
  });

  kmsCrypto.setRegion(region);

  dynamoConfig.Item.s3Prefix = {
    S : s3BucketWithPrefix
  };
  dynamoConfig.Item.loadClusters.L[0].M.targetTable = {
    S : targetTable
  };
  dynamoConfig.Item.loadClusters.L[0].M.clusterEndpoint = {
     S : clusterEndpoint
   };
  dynamoConfig.Item.loadClusters.L[0].M.clusterPort = {
     N : clusterPort
   };
  dynamoConfig.Item.loadClusters.L[0].M.clusterDB = {
     S : databaseName
   };
  dynamoConfig.Item.loadClusters.L[0].M.connectUser = {
     S : databaseUser
   };
  dynamoConfig.Item.dataFormat = {
     S : s3DataFormat
   };
  dynamoConfig.Item.jsonPath = {
     S : jsonPaths
   };
  dynamoConfig.Item.manifestBucket = {
    S : manifestBucket
  };
  dynamoConfig.Item.manifestKey = {
    S : manifestPrefix
  };
  dynamoConfig.Item.failedManifestKey = {
    S : failedManifestPrefix
  };
  dynamoConfig.Item.accessKeyForS3 = {
    S : AWSAccessKey
  };
  dynamoConfig.Item.batchSize = {
    N : '1'
  };
  dynamoConfig.Item.batchTimeoutSecs = {
    N : '3601'
  };
  dynamoConfig.Item.copyOptions = {
    S : copyOptions
  };
  callback(null);
}

encryptDB = function(callback){
  kmsCrypto.encrypt(databasePassword, function(err, ciphertext) {
    if (err) {
      console.log(JSON.stringify(err));
      process.exit(ERROR);
    } else {
      dynamoConfig.Item.loadClusters.L[0].M.connectPassword = {
        S : kmsCrypto.toLambdaStringFormat(ciphertext)
      };
      callback(null);
    }
  });
}

encryptS3 = function(callback){
  kmsCrypto.encrypt(AWSSecret, function(err, ciphertext) {
    if (err) {
      console.log(JSON.stringify(err));
      process.exit(ERROR);
    } else {
      dynamoConfig.Item.secretKeyForS3 = {
        S : kmsCrypto.toLambdaStringFormat(ciphertext)
      };
			callback(null);
    }
  });

}

createConfigs = function(callback){
    var configWriter = common.writeConfig(setRegion, dynamoDB, dynamoConfig, callback);
    	// console.log(dynamoConfig);
    common.createTables(dynamoDB, configWriter);
    callback(null);
}

qs.push(setOptions);
qs.push(encryptDB);
qs.push(encryptS3);
qs.push(createConfigs);
// call the first function in the function list, to invoke the callback
// reference chain
async.waterfall(qs);
