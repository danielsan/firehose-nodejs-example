'use strict'

// const AWS = require('aws-sdk')
// const firehose = new AWS.Firehose({ region: 'us-west-2' })
const firehose = new (require('aws-sdk/clients/firehose'))({ region: 'us-west-2' })
const env = require('./env.js')

// AWS.config.logger = process.stdout
// AWS.config.logger = console;

function createDeliveryStream (dStreamName, callback) {
  const REDSHIFT_JDBCURL = 'jdbc:redshift://' + env('REDSHIFT_HOST') + ':' + env('REDSHIFT_PORT') + '/' + env('REDSHIFT_DB')
  const
    s3config = { /* required */
      BucketARN: 'arn:aws:s3:::' + env('S3BUCKET_NAME'), /* required */
      RoleARN: `arn:aws:iam::${env('AWS_ACCOUNT_ID')}:role/firehose_delivery_role`, /* required */
      BufferingHints: {
        IntervalInSeconds: 60,
        SizeInMBs: 128
      },
      // CompressionFormat: 'UNCOMPRESSED | GZIP | ZIP | Snappy',
      CompressionFormat: 'UNCOMPRESSED',
      EncryptionConfiguration: {
        // KMSEncryptionConfig: {
        //   AWSKMSKeyARN: 'STRING_VALUE' /* required */
        // },
        NoEncryptionConfig: 'NoEncryption'
      },
      Prefix: dStreamName + '/'
    }

  const redshiftConfig = {
    DeliveryStreamName: dStreamName, /* required */
    RedshiftDestinationConfiguration: {
      ClusterJDBCURL: REDSHIFT_JDBCURL, /* required */
      CopyCommand: { /* required */
        DataTableName: dStreamName, /* required */
        CopyOptions: "FORMAT AS json 'auto'"
        // DataTableColumns: 'STRING_VALUE'
      },
      Username: env('REDSHIFT_USER'), /* required */
      Password: env('REDSHIFT_PASS'), /* required */

      // RoleARN: 'STRING_VALUE', /* required */
      // http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-kinesis-firehose
      // arn:aws:firehose:region:account-id:deliverystream/delivery-stream-name
      // RoleARN: `arn:aws:firehose:${env('AWS_REGION')}:${env('AWS_ACCOUNT_ID')}:cluster:${env('REDSHIFT_CLUSTER_NAME')}`, /* required */
      // RoleARN: `arn:aws:iam::${env('AWS_ACCOUNT_ID')}:role:firehose_delivery_role`, /* required */
      RoleARN: s3config.RoleARN,
      S3Configuration: s3config
    }
    // S3DestinationConfiguration: s3config
  }

  console.log('redshift_config', JSON.stringify(redshiftConfig, null, ' '))
  // Create the new stream if it does not already exist.
  firehose.createDeliveryStream(redshiftConfig, function (err, data) {
    if (err) { return callback(err) } // an error occurred

    callback(null, data) // successful response
  })
}

function waitForDStreamToBecomeActive (DeliveryStreamName, callback) {
  firehose.describeDeliveryStream({ DeliveryStreamName }, function (err, data) {
    if (err) return callback(err)

    if (data.DeliveryStreamDescription.DeliveryStreamStatus === 'ACTIVE') {
      console.log(DeliveryStreamName, 'is now active')
      return callback(null, data)
    }

    // The stream is not ACTIVE yet. Wait for another 5 seconds before
    // checking the state again.
    process.stdout.write('.')
    setTimeout(function () {
      waitForDStreamToBecomeActive(DeliveryStreamName, callback)
    }, 5000)
  })

  return callback.promise
}

// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Firehose.html#putRecord-property
function putRecord (dStreamName, data, callback) {
  var recordParams = {
    Record: {
      Data: JSON.stringify(data)
    },
    DeliveryStreamName: dStreamName
  }

  firehose.putRecord(recordParams, callback)
}

module.exports = {
  createDeliveryStream,
  waitForDStreamToBecomeActive,
  putRecord
}
