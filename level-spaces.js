var levelup   = require('levelup')
  , updown    = require('level-updown')
  , xtend     = require('xtend')

  , separator = '\xff'


function space (db, prefix, options) {
  if (typeof prefix != 'string')
    throw new TypeError('prefix must be a String')

  var sp      = separator + prefix + separator
    , options = xtend(options)

  function encode (key) {
    if (key == null)
      return key

    var skey = typeof key == 'string' ? key : key.toString('utf8')
    return sp + skey
  }

  function decode (key) {
    if (key == null)
      return key

    var skey = typeof key == 'string' ? key : key.toString('utf8')
    if (skey.substring(0, sp.length) === sp)
      return skey.substring(sp.length)
    return skey
  }

  function factory () {
    var ud = updown(db)

    ud.extendWith({
        prePut      : mkPrePut(encode)
      , preGet      : mkPreGet(encode)
      , postGet     : mkPostGet(decode)
      , preDel      : mkPreDel(encode)
      , preBatch    : mkPreBatch(encode)
      , preIterator : mkPreIterator(encode, decode)
    })

    return ud
  }

  if (options.keyEncoding != 'ascii')
    options.keyEncoding = 'utf8'

  options.db = factory


  return levelup(options)
}


function mkPrePut (encode) {
  return function prePut (key, value, options, callback, next) {
    next(encode(key), value, options, callback)
  }
}


function mkPreGet (encode) {
  return function preGet (key, options, callback, next) {
    next(encode(key), options, callback)
  }
}


function mkPostGet (decode) {
  return function postGet (key, options, err, value, callback, next) {
    next(decode(key), options, err, value, callback)
  }
}


function mkPreDel (encode) {
  return function preDel (key, options, callback, next) {
    next(encode(key), options, callback)
  }
}


function mkPreBatch (encode) {
  return function preBatch (array, options, callback, next) {
    var narray = array

    if (Array.isArray(array)) {
      narray = []
      for (var i = 0; i < array.length; i++) {
        narray[i] = xtend(array[i])
        narray[i].key = encode(narray[i].key)
      }
    }

    next(narray, options, callback)
  }
}


function mkPreIterator (encode, decode) {
  return function preIterator (pre) {
    var options = xtend(pre.options)

    // kudos to @dominictarr for most of this logic
    if (options.start != null || options.end != null) {
      if (!options.reverse) {
        options.gte = encode(options.start || '\x00')
        options.lte = encode(options.end   || '\xff')
      } else {
        options.gte = encode(options.end   || '\x00')
        options.lte = encode(options.start || '\xff')
      }
      delete options.start
      delete options.end
    } else { // !start && !end
      options.gte = encode(options.gte)
      options.gt  = encode(options.gt)
      options.lte = encode(options.lte)
      options.lt  = encode(options.lt)
    }

    if (options.lte == null && options.lt == null)
      options.lte = encode('\xff')
    if (options.gte == null && options.gt == null)
      options.gte = encode('\x00')

    function wrappedFactory (options) {
      var iterator = pre.factory(options)

      iterator.extendWith({
          postNext : mkPreNext(decode)
      })

      return iterator
    }

    return { options: options, factory : wrappedFactory }
  }
}


function mkPreNext (decode) {
  return function preNext (err, key, value, callback, next) {
    if (err)
      return next(err, key, value, callback)

    next(err, decode(key), value, callback)
  }
}


module.exports = space
