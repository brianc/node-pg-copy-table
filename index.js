var ok = require('okay')
var copy = require('pg-copy-streams')
try {
  var pg = require('pg')
} catch(e) {
  var pg = require('pg.js')
}

module.exports = function(config, cb) {
  var from = config.from
  var to = config.to
  var fromClient = new pg.Client(from)
  var toClient = new pg.Client(to)
  fromClient.on('drain', fromClient.end.bind(fromClient))
  toClient.on('drain', toClient.end.bind(toClient))

  fromClient.connect(ok(cb, function() {
    toClient.connect(ok(cb, function() {
      //if there is a query, make sure it is surrounded with parenthesis
      var source = from.table || ('(' + from.query + ')')

      var fromStream = fromClient.query(copy.to('COPY ' + source + ' TO STDOUT'))
      var toStream = toClient.query(copy.from('COPY ' + to.table + ' FROM STDIN'))

      //where the magic happens
      fromStream.pipe(toStream)

      //handle events for callback
      fromStream.on('end', cb)
      fromStream.once('error', function(err) {
        fromStream.removeListener('end', cb)
        toStream.removeAllListeners('error')
        cb(err)
      })
      toStream.once('error', function(err) {
        fromStream.removeAllListeners('error')
        cb(err)
      })
    }))
  }))
}
