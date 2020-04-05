'use strict'

// Loads custom ENV Variables
require('dotenv').config()

const myFirehose = require('./my-firehose.js')
const myRedshift = require('./my-redshift.js')
const myS3 = require('./my-s3.js')
const env = require('./env.js')

const dStreamName = 'test_firehose_' + ~~(Math.random() * 10000)

createS3Bucket(
  createRedshiftTable.bind(null,
    createDeliveryStream.bind(null,
      waitForDStreamToBecomeActive.bind(null,
        sendOneRecordToFirehose.bind(null,
          queryRedshiftTableEvery1min)
      ))))

function createS3Bucket (callback) {
  const bucketName = env('S3BUCKET_NAME')
  console.log('createS3Bucket', bucketName)

  myS3.createBucketIfItDoesNotExist(bucketName, function (err, res) {
    if (err) throw new Error(err)

    callback(null, res)
  })
}

function createRedshiftTable (callback) {
  console.log('createRedshiftTable')
  const sqlString = `
  CREATE TABLE ${dStreamName} (
    id INT,
    name VARCHAR(200),
    created_date DATE,
    created_at TIMESTAMP
  );`
  console.log(sqlString)

  myRedshift.createTable(sqlString, function (err, res) {
    if (err) throw new Error(err)

    callback(null, res)
  })
}

function createDeliveryStream (callback) {
  console.log('createDeliveryStream')
  myFirehose.createDeliveryStream(dStreamName, function (err, res) {
    if (err) throw new Error(err)

    callback(null, res)
  })
}

function waitForDStreamToBecomeActive (callback) {
  console.log('waitForDStreamToBecomeActive')
  myFirehose.waitForDStreamToBecomeActive(dStreamName, function (err, res) {
    if (err) throw new Error(err)

    callback(null, res)
  })
}

function sendOneRecordToFirehose (callback) {
  console.log('sendOneRecordToFirehose')
  const now = new Date().toISOString()
  const record = {
    id: 1,
    name: 'Daniel San',
    created_date: now.substr(0, 10),
    created_at: now
  }

  console.log(record)

  myFirehose.putRecord(dStreamName, record, function (err, res) {
    if (err) throw new Error(err)

    callback(null, res)
  })
}

function queryRedshiftTableEvery1min () {
  console.log('\nqueryRedshiftTableEvery1min', new Date())

  myRedshift.selectAllFrom(dStreamName, function (err, res) {
    if (err) throw new Error(err)

    console.log('rows', res.rows)
    if (res.rows.length === 0) {
      return setTimeout(queryRedshiftTableEvery1min, 60000, dStreamName)
    }

    console.log('There is your record :D')
  })
}
