var levelup = require('levelup')
  , spaces  = require('../')
  , after   = require('after')

var db       = levelup('foo.db')
  , space1   = spaces(db, 'space 1')
  , space2   = spaces(db, 'space 2')
  , space1_1 = spaces(space1, 'space 1.1')

var done = after(4, dump)

;[ db, space1, space2, space1_1 ].forEach(function (db, i) {
  var key = 'foo ' + i
  db.put(key, 'bar ' + i, function () {
    db.get(key, function (err, value) {
      console.log('[%s] = [%s]', key, value)
      done()
    })
  })
})

function dump () {
  db.createReadStream().on('data', console.log)
}