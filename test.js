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
  return function (expected) {
    readStreamToList(ldb.createReadStream(), function (err, data) {
      t.ifError(err, 'no error')

      t.deepEqual(data, expected, 'database contains expected entries')
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
        { 'bar0'                     : 'foo0' }
      , { 'foo0'                     : 'bar0' }
      , { '\xfftest space 1\xffbar1' : 'foo1' }
      , { '\xfftest space 1\xfffoo1' : 'bar1' }
      , { '\xfftest space 2\xffbar2' : 'foo2' }
      , { '\xfftest space 2\xfffoo2' : 'bar2' }
    ])

    t.end()
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
        { 'bar0'                                          : 'foo0' }
      , { 'foo0'                                          : 'bar0' }
      , { '\xfftest space 1\xffbar1'                      : 'foo1' }
      , { '\xfftest space 1\xfffoo1'                      : 'bar1' }
      , { '\xfftest space 1\xff\xffinner space 1\xffbar3' : 'foo3' }
      , { '\xfftest space 1\xff\xffinner space 1\xfffoo3' : 'bar3' }
      , { '\xfftest space 1\xff\xffinner space 2\xffbar4' : 'foo4' }
      , { '\xfftest space 1\xff\xffinner space 2\xfffoo4' : 'bar4' }
      , { '\xfftest space 2\xffbar2'                      : 'foo2' }
      , { '\xfftest space 2\xfffoo2'                      : 'bar2' }
      , { '\xfftest space 2\xff\xffinner space 1\xffbar5' : 'foo5' }
      , { '\xfftest space 2\xff\xffinner space 1\xfffoo5' : 'bar5' }
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
      , { '\xfftest space 1\xfffoo1' : 'bar1' }
      , { '\xfftest space 2\xfffoo2' : 'bar2' }
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
        { 'foo0'                                          : 'bar0' }
      , { '\xfftest space 1\xfffoo1'                      : 'bar1' }
      , { '\xfftest space 1\xff\xffinner space 1\xfffoo3' : 'bar3' }
      , { '\xfftest space 1\xff\xffinner space 2\xfffoo4' : 'bar4' }
      , { '\xfftest space 2\xfffoo2'                      : 'bar2' }
      , { '\xfftest space 2\xff\xffinner space 1\xfffoo5' : 'bar5' }
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
        { 'bang0'                     : 'boom0' }
      , { 'boom0'                     : 'bang0' }
      , { 'foo0'                      : 'bar0' }
      , { '\xfftest space 1\xffbang1' : 'boom1' }
      , { '\xfftest space 1\xffboom1' : 'bang1' }
      , { '\xfftest space 1\xfffoo1'  : 'bar1' }
      , { '\xfftest space 2\xffbang2' : 'boom2' }
      , { '\xfftest space 2\xffboom2' : 'bang2' }
      , { '\xfftest space 2\xfffoo2'  : 'bar2' }
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
        { 'bang0'                                          : 'boom0' }
      , { 'boom0'                                          : 'bang0' }
      , { 'foo0'                                           : 'bar0' }
      , { '\xfftest space 1\xffbang1'                      : 'boom1' }
      , { '\xfftest space 1\xffboom1'                      : 'bang1' }
      , { '\xfftest space 1\xfffoo1'                       : 'bar1' }
      , { '\xfftest space 1\xff\xffinner space 1\xffbang3' : 'boom3' }
      , { '\xfftest space 1\xff\xffinner space 1\xffboom3' : 'bang3' }
      , { '\xfftest space 1\xff\xffinner space 1\xfffoo3'  : 'bar3' }
      , { '\xfftest space 1\xff\xffinner space 2\xffbang4' : 'boom4' }
      , { '\xfftest space 1\xff\xffinner space 2\xffboom4' : 'bang4' }
      , { '\xfftest space 1\xff\xffinner space 2\xfffoo4'  : 'bar4' }
      , { '\xfftest space 2\xffbang2'                      : 'boom2' }
      , { '\xfftest space 2\xffboom2'                      : 'bang2' }
      , { '\xfftest space 2\xfffoo2'                       : 'bar2' }
      , { '\xfftest space 2\xff\xffinner space 1\xffbang5' : 'boom5' }
      , { '\xfftest space 2\xff\xffinner space 1\xffboom5' : 'bang5' }
      , { '\xfftest space 2\xff\xffinner space 1\xfffoo5'  : 'bar5' }
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
        { 'bang0'                     : 'boom0' }
      , { 'boom0'                     : 'bang0' }
      , { 'foo0'                      : 'bar0' }
      , { '\xfftest space 1\xffbang1' : 'boom1' }
      , { '\xfftest space 1\xffboom1' : 'bang1' }
      , { '\xfftest space 1\xfffoo1'  : 'bar1' }
      , { '\xfftest space 2\xffbang2' : 'boom2' }
      , { '\xfftest space 2\xffboom2' : 'bang2' }
      , { '\xfftest space 2\xfffoo2'  : 'bar2' }
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
        { 'bang0'                                          : 'boom0' }
      , { 'boom0'                                          : 'bang0' }
      , { 'foo0'                                           : 'bar0' }
      , { '\xfftest space 1\xffbang1'                      : 'boom1' }
      , { '\xfftest space 1\xffboom1'                      : 'bang1' }
      , { '\xfftest space 1\xfffoo1'                       : 'bar1' }
      , { '\xfftest space 1\xff\xffinner space 1\xffbang3' : 'boom3' }
      , { '\xfftest space 1\xff\xffinner space 1\xffboom3' : 'bang3' }
      , { '\xfftest space 1\xff\xffinner space 1\xfffoo3'  : 'bar3' }
      , { '\xfftest space 1\xff\xffinner space 2\xffbang4' : 'boom4' }
      , { '\xfftest space 1\xff\xffinner space 2\xffboom4' : 'bang4' }
      , { '\xfftest space 1\xff\xffinner space 2\xfffoo4'  : 'bar4' }
      , { '\xfftest space 2\xffbang2'                      : 'boom2' }
      , { '\xfftest space 2\xffboom2'                      : 'bang2' }
      , { '\xfftest space 2\xfffoo2'                       : 'bar2' }
      , { '\xfftest space 2\xff\xffinner space 1\xffbang5' : 'boom5' }
      , { '\xfftest space 2\xff\xffinner space 1\xffboom5' : 'bang5' }
      , { '\xfftest space 2\xff\xffinner space 1\xfffoo5'  : 'bar5' }
    ])  

    t.end()
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


function readStreamTest (options) {
  test('test readStream with ' + inspect(options), function (t) {
    var refDb = levelup(testDb + '.ref')
      , ldb   = levelup(testDb)
      , sdb1  = spaces(ldb, 'test space')
      , sdb2  = spaces(sdb1, 'inner space ')
      , refList
      , sdb1List
      , sdb2List
      , done  = after(2, prepare)

    refDb.on('ready', done)
    ldb.on('ready', done)

    function prepare () {
      var batches = [ refDb.batch(), ldb.batch(), sdb1.batch(), sdb2.batch() ]
        , done    = after(batches.length, exec)

      for (var i = 0; i < 200; i++) {
        batches.forEach(function (batch) {
          batch.put('key' + i, 'value' + i)
        })
      }

      batches.forEach(function (batch) {
        batch.write(done)
      })
    }

    function exec () {
      var done = after(3, verify)

      readStreamToList(refDb.createReadStream(options), function (err, data) {
        t.ifError(err, 'no error')
        refList = data
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
      var done = after(2, function (err) {
        t.ifError(err, 'no error')
        rimraf.sync(testDb)
        rimraf.sync(testDb + '.ref')
        t.end()
      })

      t.equal(sdb1List.length, refList.length, 'space db returned correct number of entries (' + refList.length + ')')
      t.equal(sdb2List.length, refList.length, 'inner space db returned correct number of entries (' + refList.length + ')')
      t.deepEqual(sdb1List, refList, 'space db returned same entries as reference db')
      t.deepEqual(sdb2List, refList, 'inner space db returned same entries as reference db')     

      refDb.close(done)
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

