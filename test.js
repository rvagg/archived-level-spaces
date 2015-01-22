var test    = require('tape')
  , spaces  = require('./')
  , levelup = require('levelup')
  , rimraf  = require('rimraf')
  , list    = require('list-stream')
  , after   = require('after')
  , inspect = require('util').inspect

  , testDb  = '__test.db'


function readStreamToList (readStream, callback) {
  readStream.pipe(list.obj(function (err, data) {
    if (err)
      return callback(err)

    data = data.map(function (entry) {
      var o = {}
      o[entry.key] = entry.value
      return o
    })

    callback(null, data)
  }))
}


function dbEquals (ldb, t) {
  return function (expected, callback) {
    readStreamToList(ldb.createReadStream(), function (err, data) {
      t.ifError(err, 'no error')
      t.deepEqual(data, expected, 'database contains expected entries')
      callback()
    })
  }
}


function dbWrap (testFn) {
  return function (t) {
    levelup(testDb, function (err, ldb) {
      t.ifError(err, 'no error')

      t.$end = t.end
      t.end = function (err) {
        if (err !== undefined)
          t.ifError(err, 'no error')

        ldb.close(function (err) {
          t.ifError(err, 'no error')
          rimraf.sync(testDb)
          t.$end()
        })
      }
      t.dbEquals = dbEquals(ldb, t)

      testFn(t, ldb)
    })
  }
}

test('test puts', dbWrap(function (t, ldb) {
  var dbs = [
          ldb
        , spaces(ldb, 'test space 1')
        , spaces(ldb, 'test space 2')
      ]
    , done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'bar0'               : 'foo0' }
      , { 'foo0'               : 'bar0' }
      , { '~test space 1~bar1' : 'foo1' }
      , { '~test space 1~foo1' : 'bar1' }
      , { '~test space 2~bar2' : 'foo2' }
      , { '~test space 2~foo2' : 'bar2' }
    ], t.end)
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test separator', dbWrap(function (t, ldb) {
  var idb
    , dbs = [
          ldb
        , spaces(ldb, 'test space 1', { separator: 'M' })
        , idb = spaces(ldb, 'test space 2', { separator: ';' })
        , spaces(idb, 'inner space', { separator: '*' })
      ]
    , done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { ';test space 2;*inner space*bar3' : 'foo3' }
      , { ';test space 2;*inner space*foo3' : 'bar3' }
      , { ';test space 2;bar2' : 'foo2' }
      , { ';test space 2;foo2' : 'bar2' }
      , { 'Mtest space 1Mbar1' : 'foo1' }
      , { 'Mtest space 1Mfoo1' : 'bar1' }
      , { 'bar0'               : 'foo0' }
      , { 'foo0'               : 'bar0' }
    ], t.end)
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test puts @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1  = spaces(ldb, 'test space 1')
    , sdb2  = spaces(ldb, 'test space 2')
    , sdb11 = spaces(sdb1, 'inner space 1')
    , sdb12 = spaces(sdb1, 'inner space 2')
    , sdb21 = spaces(sdb2, 'inner space 1')
    , dbs   = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    , done  = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'bar0'                              : 'foo0' }
      , { 'foo0'                              : 'bar0' }
      , { '~test space 1~bar1'                : 'foo1' }
      , { '~test space 1~foo1'                : 'bar1' }
      , { '~test space 1~~inner space 1~bar3' : 'foo3' }
      , { '~test space 1~~inner space 1~foo3' : 'bar3' }
      , { '~test space 1~~inner space 2~bar4' : 'foo4' }
      , { '~test space 1~~inner space 2~foo4' : 'bar4' }
      , { '~test space 2~bar2'                : 'foo2' }
      , { '~test space 2~foo2'                : 'bar2' }
      , { '~test space 2~~inner space 1~bar5' : 'foo5' }
      , { '~test space 2~~inner space 1~foo5' : 'bar5' }
    ])  

    t.end()
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test gets', dbWrap(function (t, ldb) {
  var dbs = [
          ldb
        , spaces(ldb, 'test space 1')
        , spaces(ldb, 'test space 2')
      ]
    , done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length * 2, t.end)

    dbs.forEach(function (db, i) {
      db.get('foo' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'bar' + i, 'got expected value')
        done()
      })
      db.get('bar' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'foo' + i, 'got expected value')
        done()
      })
    })
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test gets @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1  = spaces(ldb, 'test space 1')
    , sdb2  = spaces(ldb, 'test space 2')
    , sdb11 = spaces(sdb1, 'inner space 1')
    , sdb12 = spaces(sdb1, 'inner space 2')
    , sdb21 = spaces(sdb2, 'inner space 1')
    , dbs   = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    , done  = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length * 2, t.end)

    dbs.forEach(function (db, i) {
      db.get('foo' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'bar' + i, 'got expected value')
        done()
      })
      db.get('bar' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'foo' + i, 'got expected value')
        done()
      })
    })
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test dels', dbWrap(function (t, ldb) {
  var dbs = [
          ldb
        , spaces(ldb, 'test space 1')
        , spaces(ldb, 'test space 2')
      ]
    , done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.del('bar' + i, function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'foo0'                     : 'bar0' }
      , { '~test space 1~foo1' : 'bar1' }
      , { '~test space 2~foo2' : 'bar2' }
    ])

    t.end()
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test dels @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1  = spaces(ldb, 'test space 1')
    , sdb2  = spaces(ldb, 'test space 2')
    , sdb11 = spaces(sdb1, 'inner space 1')
    , sdb12 = spaces(sdb1, 'inner space 2')
    , sdb21 = spaces(sdb2, 'inner space 1')
    , dbs   = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    , done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.del('bar' + i, function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'foo0'                              : 'bar0' }
      , { '~test space 1~foo1'                : 'bar1' }
      , { '~test space 1~~inner space 1~foo3' : 'bar3' }
      , { '~test space 1~~inner space 2~foo4' : 'bar4' }
      , { '~test space 2~foo2'                : 'bar2' }
      , { '~test space 2~~inner space 1~foo5' : 'bar5' }
    ])  

    t.end()
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test batch', dbWrap(function (t, ldb) {
  var dbs = [
          ldb
        , spaces(ldb, 'test space 1')
        , spaces(ldb, 'test space 2')
      ]
    , done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch([
          { type: 'put', key: 'boom' + i, value: 'bang' + i }
        , { type: 'del', key: 'bar' + i }
        , { type: 'put', key: 'bang' + i, value: 'boom' + i }
      ], function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'bang0'               : 'boom0' }
      , { 'boom0'               : 'bang0' }
      , { 'foo0'                : 'bar0' }
      , { '~test space 1~bang1' : 'boom1' }
      , { '~test space 1~boom1' : 'bang1' }
      , { '~test space 1~foo1'  : 'bar1' }
      , { '~test space 2~bang2' : 'boom2' }
      , { '~test space 2~boom2' : 'bang2' }
      , { '~test space 2~foo2'  : 'bar2' }
    ])

    t.end()
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test batch @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1  = spaces(ldb, 'test space 1')
    , sdb2  = spaces(ldb, 'test space 2')
    , sdb11 = spaces(sdb1, 'inner space 1')
    , sdb12 = spaces(sdb1, 'inner space 2')
    , sdb21 = spaces(sdb2, 'inner space 1')
    , dbs   = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    , done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch([
          { type: 'put', key: 'boom' + i, value: 'bang' + i }
        , { type: 'del', key: 'bar' + i }
        , { type: 'put', key: 'bang' + i, value: 'boom' + i }
      ], function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'bang0'                              : 'boom0' }
      , { 'boom0'                              : 'bang0' }
      , { 'foo0'                               : 'bar0' }
      , { '~test space 1~bang1'                : 'boom1' }
      , { '~test space 1~boom1'                : 'bang1' }
      , { '~test space 1~foo1'                 : 'bar1' }
      , { '~test space 1~~inner space 1~bang3' : 'boom3' }
      , { '~test space 1~~inner space 1~boom3' : 'bang3' }
      , { '~test space 1~~inner space 1~foo3'  : 'bar3' }
      , { '~test space 1~~inner space 2~bang4' : 'boom4' }
      , { '~test space 1~~inner space 2~boom4' : 'bang4' }
      , { '~test space 1~~inner space 2~foo4'  : 'bar4' }
      , { '~test space 2~bang2'                : 'boom2' }
      , { '~test space 2~boom2'                : 'bang2' }
      , { '~test space 2~foo2'                 : 'bar2' }
      , { '~test space 2~~inner space 1~bang5' : 'boom5' }
      , { '~test space 2~~inner space 1~boom5' : 'bang5' }
      , { '~test space 2~~inner space 1~foo5'  : 'bar5' }
    ])  

    t.end()
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test chained batch', dbWrap(function (t, ldb) {
  var dbs = [
          ldb
        , spaces(ldb, 'test space 1')
        , spaces(ldb, 'test space 2')
      ]
    , done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch()
        .put('boom' + i, 'bang' + i)
        .del('bar' + i)
        .put('bang' + i, 'boom' + i)
        .write(function (err) {
          t.ifError(err, 'no error')
          done()
        })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'bang0'               : 'boom0' }
      , { 'boom0'               : 'bang0' }
      , { 'foo0'                : 'bar0' }
      , { '~test space 1~bang1' : 'boom1' }
      , { '~test space 1~boom1' : 'bang1' }
      , { '~test space 1~foo1'  : 'bar1' }
      , { '~test space 2~bang2' : 'boom2' }
      , { '~test space 2~boom2' : 'bang2' }
      , { '~test space 2~foo2'  : 'bar2' }
    ])

    t.end()
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test batch @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1  = spaces(ldb, 'test space 1')
    , sdb2  = spaces(ldb, 'test space 2')
    , sdb11 = spaces(sdb1, 'inner space 1')
    , sdb12 = spaces(sdb1, 'inner space 2')
    , sdb21 = spaces(sdb2, 'inner space 1')
    , dbs   = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    , done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch()
        .put('boom' + i, 'bang' + i)
        .del('bar' + i)
        .put('bang' + i, 'boom' + i)
        .write(function (err) {
          t.ifError(err, 'no error')
          done()
        })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
        { 'bang0'                              : 'boom0' }
      , { 'boom0'                              : 'bang0' }
      , { 'foo0'                               : 'bar0' }
      , { '~test space 1~bang1'                : 'boom1' }
      , { '~test space 1~boom1'                : 'bang1' }
      , { '~test space 1~foo1'                 : 'bar1' }
      , { '~test space 1~~inner space 1~bang3' : 'boom3' }
      , { '~test space 1~~inner space 1~boom3' : 'bang3' }
      , { '~test space 1~~inner space 1~foo3'  : 'bar3' }
      , { '~test space 1~~inner space 2~bang4' : 'boom4' }
      , { '~test space 1~~inner space 2~boom4' : 'bang4' }
      , { '~test space 1~~inner space 2~foo4'  : 'bar4' }
      , { '~test space 2~bang2'                : 'boom2' }
      , { '~test space 2~boom2'                : 'bang2' }
      , { '~test space 2~foo2'                 : 'bar2' }
      , { '~test space 2~~inner space 1~bang5' : 'boom5' }
      , { '~test space 2~~inner space 1~boom5' : 'bang5' }
      , { '~test space 2~~inner space 1~foo5'  : 'bar5' }
    ])  

    t.end()
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))

test('works with options.valueEncoding: json', dbWrap(function (t, ldb) {
  var thing = { one: 'two', three: 'four' }
  var opt = {valueEncoding: 'json'}
  var jsonDb = spaces(ldb, 'json-things', opt)

  jsonDb.put('thing', thing, opt, function (err) {
    t.ifError(err, 'no error')

    jsonDb.get('thing', opt, function (err, got) {
      t.ifError(err, 'no error')
      t.ok(got, 'got something back!')
      t.equal(typeof got, 'object', 'got back an object') //this currently fails
      t.deepEqual(got, thing, 'got back the right thing') //this currently fails
      t.end()
    })
  })
}))


function readStreamTest (options) {
  test('test readStream with ' + inspect(options), function (t) {
    var ref1Db = levelup(testDb + '.ref')
      , ref2Db = levelup(testDb + '.ref2')
      , ldb   = levelup(testDb)
      , sdb1  = spaces(ldb, 'test space')
      , sdb2  = spaces(sdb1, 'inner space ')
      , ref1List
      , ref2List
      , sdb1List
      , sdb2List
      , done  = after(3, prepare)

    ref1Db.on('ready', done)
    ref2Db.on('ready', done)
    ldb.on('ready', done)

    function prepare () {
      var ref1Batch = ref1Db.batch()
        , batches   = [ ref1Batch, ref2Db.batch(), ldb.batch(), sdb1.batch(), sdb2.batch() ]
        , done      = after(batches.length, exec)

      for (var i = 0; i < 200; i++) {
        batches.forEach(function (batch) {
          batch.put('key' + i, 'value' + i)
        })
        // we simulate the inner space in a separate db, not trying to hide it
        ref1Batch.put('~inner space ~key' + i, 'value' + i)
      }

      batches.forEach(function (batch) {
        batch.write(done)
      })
    }

    function exec () {
      var done = after(4, verify)

      readStreamToList(ref1Db.createReadStream(options), function (err, data) {
        t.ifError(err, 'no error')
        ref1List = data
        done()
      })

      readStreamToList(ref2Db.createReadStream(options), function (err, data) {
        t.ifError(err, 'no error')
        ref2List = data
        done()
      })

      readStreamToList(sdb1.createReadStream(options), function (err, data) {
        t.ifError(err, 'no error')
        sdb1List = data
        done()
      })

      readStreamToList(sdb2.createReadStream(options), function (err, data) {
        t.ifError(err, 'no error')
        sdb2List = data
        done()
      })
    }

    function verify () {
      var done = after(3, function (err) {
        t.ifError(err, 'no error')
        rimraf.sync(testDb)
        rimraf.sync(testDb + '.ref')
        t.end()
      })

      t.equal(
          sdb1List.length
        , ref1List.length
        , 'inner space db returned correct number of entries (' + ref1List.length + ')'
      )
      t.deepEqual(sdb1List, ref1List, 'inner space db returned same entries as reference db')     

      t.equal(
          sdb2List.length
        , ref2List.length
        , 'inner space db returned correct number of entries (' + ref2List.length + ')'
      )
      t.deepEqual(sdb2List, ref2List, 'inner space db returned same entries as reference db')     

      ref1Db.close(done)
      ref2Db.close(done)
      ldb.close(done)
    }
  })
}

readStreamTest({})
readStreamTest({ start: 'key0', end: 'key50' })
readStreamTest({ start: 'key0', end: 'key150' })
readStreamTest({ gte: 'key0', lte: 'key50' })
readStreamTest({ gt: 'key0', lt: 'key50' })
readStreamTest({ gte: 'key0', lte: 'key150' })
readStreamTest({ gt: 'key0', lt: 'key150' })
readStreamTest({ start: 'key0', end: 'key50' })
readStreamTest({ start: 'key50', end: 'key150' })
readStreamTest({ start: 'key50' })
readStreamTest({ end: 'key50' })
readStreamTest({ gt: 'key50' })
readStreamTest({ gte: 'key50' })
readStreamTest({ lt: 'key50' })
readStreamTest({ lte: 'key50' })
readStreamTest({ reverse: true })
readStreamTest({ start: 'key0', end: 'key50', reverse: true })
readStreamTest({ start: 'key50', end: 'key150', reverse: true })
readStreamTest({ gte: 'key0', lte: 'key50', reverse: true })
readStreamTest({ gt: 'key0', lt: 'key50', reverse: true })
readStreamTest({ gte: 'key0', lte: 'key150', reverse: true })
readStreamTest({ gt: 'key0', lt: 'key150', reverse: true })
readStreamTest({ start: 'key50', reverse: true })
readStreamTest({ end: 'key50', reverse: true })
readStreamTest({ gt: 'key50', reverse: true })
readStreamTest({ gte: 'key50', reverse: true })
readStreamTest({ lt: 'key50', reverse: true })
readStreamTest({ lte: 'key50', reverse: true })
readStreamTest({ limit: 40 })
readStreamTest({ start: 'key0', end: 'key50', limit: 40 })
readStreamTest({ start: 'key50', end: 'key150', limit: 40 })
readStreamTest({ start: 'key50', limit: 40 })
readStreamTest({ reverse: true, limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key50', limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key50', limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key150', limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key150', limit: 40 })
readStreamTest({ start: 'key50', limit: 40 })
readStreamTest({ end: 'key50', limit: 40 })
readStreamTest({ gt: 'key50', limit: 40 })
readStreamTest({ gte: 'key50', limit: 40 })
readStreamTest({ lt: 'key50', limit: 40 })
readStreamTest({ lte: 'key50', limit: 40 })
readStreamTest({ start: 'key0', end: 'key50', reverse: true, limit: 40 })
readStreamTest({ start: 'key50', end: 'key150', reverse: true, limit: 40 })
readStreamTest({ start: 'key50', reverse: true, limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key50', reverse: true, limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key50', reverse: true, limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key150', reverse: true, limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key150', reverse: true, limit: 40 })
readStreamTest({ start: 'key50', reverse: true, limit: 40 })
readStreamTest({ end: 'key50', reverse: true, limit: 40 })
readStreamTest({ gt: 'key50', reverse: true, limit: 40 })
readStreamTest({ gte: 'key50', reverse: true, limit: 40 })
readStreamTest({ lt: 'key50', reverse: true, limit: 40 })
readStreamTest({ lte: 'key50', reverse: true, limit: 40 })