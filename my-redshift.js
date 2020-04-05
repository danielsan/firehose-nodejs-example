'use strict'

// const client = require('pg')

const redshiftConf = {
  host: process.env.REDSHIFT_HOST,
  port: process.env.REDSHIFT_PORT || 5439, // 5439 is the default redshift port
  database: process.env.REDSHIFT_DB,
  user: process.env.REDSHIFT_USER,
  password: process.env.REDSHIFT_PASS
}

const getClient = () => new (require('pg').Client)(redshiftConf)

function createTable (sqlString, callback) {
  // client.connect(redshiftConf, function pgConnect (err, client, done) {
  const client = getClient()
  client.connect(function pgConnect (err) {
    if (err) return callback(err)

    client.query(sqlString, function createTableCallback (err, result) {
      // call `done()` to release the client back to the pool
      // done()

      // close the connection
      client.end(endErr => {
        console.log('closing connection')
        if (err || endErr) return callback(err || endErr)

        callback(null, result)
      })
    })
  })
}

function selectAllFrom (tableName, callback) {
  const sqlString = 'SELECT * FROM ' + tableName + ' LIMIT 10;'
  console.log({ sql_string: sqlString })

  // pg.connect(redshift_conf, function pg_connect(err, client, done) {
  const client = getClient()
  client.connect(function pgConnect (err) {
    if (err) return callback(err)

    client.query(sqlString, function createTableCallback (err, result) {
      // close the connection
      client.end(endErr => {
        console.log('closing connection')
        if (err || endErr) return callback(err || endErr)

        callback(null, result)
      })
    })
  })
}

module.exports = {
  createTable,
  selectAllFrom
}
