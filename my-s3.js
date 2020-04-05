'use strict'

// const AWS = require('aws-sdk')
// const S3 = new AWS.S3({ region: 'us-west-2' })
const S3 = new (require('aws-sdk/clients/s3'))({ region: 'us-west-2' })
const env = require('./env.js')

function checkIfBucketExists (bucketName, callback) {
  S3.listBuckets(function (err, buckets) {
    if (err) return callback(err)

    const bucketNames = buckets.Buckets.map(b => b.Name)
    const bucketOIndex = bucketNames.indexOf(bucketName)
    if (bucketOIndex === -1) { return callback() }

    callback(null, buckets.Buckets[bucketOIndex])
  })
}

function createBucketIfItDoesNotExist (bucketName, callback) {
  const s3Config = {
    Bucket: 'firehose-nodejs-example',
    CreateBucketConfiguration: {
      LocationConstraint: env('AWS_REGION')
    }
  }

  checkIfBucketExists(bucketName, function (err, bucket) {
    if (err) return callback(err)

    if (bucket) return callback(null, bucket)

    S3.createBucket(s3Config, function (err, bucket) {
      if (err) return callback(err)

      callback(null, bucket)
    })
  })
}

module.exports = {
  checkIfBucketExists,
  createBucketIfItDoesNotExist
}
