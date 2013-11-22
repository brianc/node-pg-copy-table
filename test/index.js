var ok = require('okay')
var assert = require('assert')
var Client = require('pg.js').Client
var copy = require('../')

var run = function(database, commands, cb) {
  var client = new Client({
    database: database
  })
  client.connect()
  commands.forEach(function(cmd) {
    client.query(cmd)
  })
  client.on('drain', function() {
    client.end()
    cb()
  })
}

var query = function(database, text, cb) {
  var client = new Client({database: database})
  client.connect()
  client.on('drain', client.end.bind(client))
  client.query(text, cb)
}

var createTable = function(database, cb) {
  var commands = ['create table if not exists blah(num int)']
  commands.push('delete from blah')
  commands.unshift('drop table if exists blah')
  run(database, commands, cb)
}

var dropTable = function(database, cb) {
  run(database, ['drop table if exists blah'], cb)
}

describe('pg-copy-table', function() {

  before(function(done) {
    createTable('asdf', function() {
      createTable('postgres', done)
    })
  })

  after(function(done) {
    dropTable('asdf', function() {
      dropTable('postgres', done)
    })
  })

  it('works', function(done) {
    var config = {
      from: {
        database: 'asdf',
        query: 'SELECT generate_series as num from generate_series(0, 1000)'
      },
      to: {
        database: 'postgres',
        table: 'blah'
      }
    }
    copy(config, function(err) {
      if(err) return done(err);
      query('postgres', 'SELECT COUNT(*) FROM blah', ok(done, function(res) {
        var rows = res.rows
        assert.equal(rows[0].count, 1001)
        require('pg.js').end()
        done()
      }))
    })
  })

  it('handles connection error', function(done) {
    var config = {
      from: {
        database: 'alksjdflaksjdflaksdf'
      },
      to: {
        database: 'lkasjldkfjlaksjdfadsf'
      }
    }
    copy(config, function(err) {
      assert(err)
      done()
    })
  })

  it('handles error on from stream', function(done) {
    var config = {
      from: {
        database: 'asdf',
        query: 'asdf'
      },
      to: {
        database: 'asdf',
        table: 'blah'
      }
    }
    copy(config, function(err) {
      assert(err)
      done()
    })
  })

  it('handles error on to stream', function(done) {
    var config = {
      from: {
        database: 'asdf',
        query: 'blah'
      },
      to: {
        database: 'asdf',
        table: 'asdfasdfasdf'
      }
    }
    copy(config, function(err) {
      assert(err)
      done()
    })
  })


  it('connects to different databases', function(done) {
    var config = {
      from: {
        database: 'postgres',
        table: 'blah'
      },
      to: {
        database: 'asdf',
        table: 'blah'
      }
    }
    copy(config, ok(done, function() {
      query('asdf', 'SELECT COUNT(*) FROM blah', ok(done, function(res) {
        var row = res.rows[0]
        assert.equal(row.count, 1001)
        done()
      }))
    }))
  })
})
