# level-spaces

**A simple namespacing solution for LevelUP**

[![NPM](https://nodei.co/npm/level-spaces.png?downloads=true&downloadRank=true)](https://nodei.co/npm/level-spaces/)
[![NPM](https://nodei.co/npm-dl/level-spaces.png?months=6&height=3)](https://nodei.co/npm/level-spaces/)

## Why?

Inspired by [level-sublevel](https://github.com/dominictarr/level-sublevel), **level-spaces** provides a simple mechanism for creating namespaced *views* of a **[LevelUP](https://github.com/rvagg/node-levelup)** data store. At a basic level this can be thought of as a way to provide the k/v store equivalent of relational database "tables".

**level-spaces** is aimed at basic use-cases and does not introduce any versioning problems by modifying the original LevelUP instance. Each instance of **level-spaces** is independent and therefore an application may include many separate versions of **level-spaces** in use by different packages. This makes it an ideal solution for storing metadata in LevelUP extensions (such as that in [level-ttl](https://github.com/rvagg/node-level-ttl/)). **level-spaces** does not provide direct mechanisms to intercept writes and turn them into batches *across* namespaces. If you need to write metadata atomically then you should consider **[level-sublevel](https://github.com/rvagg/level-sublevel)** instead.

**level-spaces** uses **[level-updown](https://github.com/rvagg/level-updown)** to implement its functionality and it can therefore return a new LevelUP instance, unmodified (un-*monkey-patched*!) in any way. You can then use that LevelUP instance to plug in additional LevelUP extensions and pass it around as if it was an entirely independent LevelUP store. In fact, you can even pass the new LevelUP instance back in to **level-spaces** to get hierarchical namespaces built on multiple levels of LevelUP / level-spaces / level-updown / LevelDOWN.

## How?

Currently **level-spaces** **only supports `String` keys** (UTF-8), although LevelUP may pass it stringified JSON objects as keys as well (this is transparent). If you have more complicated needs for your keys then you should consider **[level-sublevel](https://github.com/rvagg/level-sublevel)** instead.

A namespace is specified as a `String`. This `String` has the character `\xff` (`'ÿ'`) prepended to the beginning and appended to the end and is then *prefixed* to all reads and writes to the underlying LevelUP.

So, if you have a namespace of `'foobar'`, all keys written will transparently be prefixed with `'\xfffoobar\xff'` and all reads will have keys transparently prefixed with `'\xfffoobar\xff'`. All keys that come from an iterator / read-stream will also have `'\xfffoobar\xff'` removed from them, making the prefixing entirely transparent. You will not be able to see the raw keys from a **level-spaces** instance but they will be visible from the LevelUP used to create it.

If you have multiple levels of **level-spaces** you will end up with multiple prefixes appended one after the other, each surrounded by `\xff`. So a **level-spaces** instance with the prefix `'foobar'` that is passed back in to create a new **level-spaces** instance with a prefix `'doobar'` will end up using keys prefixed with `\xfffoobar\xff\xffdoobar\xff`.

Additionally, when you call `createReadStream()` on a LevelUP created by **level-spaces**, the options will be rewritten to properly account for the underlying namespace: `'start'`, `'end'`, `'gt'`, `'gte'`, `'lt'`, `'lte'`, so the LevelUP read-stream operates only within the namespace as if there was no other range of keys in the store.

## Examples

```js
var levelup = require('levelup')
  , spaces  = require('../')
  , after   = require('after')

var db       = levelup('foo.db')
  , space1   = spaces(db, 'space 1')
  , space2   = spaces(db, 'space 2')
  , space1_1 = spaces(space1, 'space 1.1')

var done = after(4, dump)

;[ db, space1, space2, space1_1 ].forEach(function (db) {
  db.put('foo', 'bar', function () {
    db.get('foo', function (err, value) {
      console.log('[%s] = [%s]', 'foo', value)
      done()
    })
  })
})

function dump () {
  db.createReadStream().on('data', console.log)
}
```

Gives us the output:

```text
[foo 0] = [bar 0]
[foo 1] = [bar 1]
[foo 2] = [bar 2]
[foo 3] = [bar 3]
{ key: 'foo 0', value: 'bar 0' }
{ key: 'ÿspace 1ÿfoo 1', value: 'bar 1' }
{ key: 'ÿspace 1ÿÿspace 1.1ÿfoo 3', value: 'bar 3' }
{ key: 'ÿspace 2ÿfoo 2', value: 'bar 2' }
```

Note how each LevelUP instance appears to be writing and reading in the same way but the underlying data store has namespaced keys.

## API

### spaces(db, namespace[, options])

Create a new LevelUP instance from an existing one (supplied as `db`) where keys are namespaced into the `namespace` string.

The optional `options` object will be passed to LevelUP.

## License

**level-spaces** is Copyright (c) 2014 Rod Vagg [@rvagg](https://twitter.com/rvagg) and licensed under the MIT licence. All rights not explicitly granted in the MIT license are reserved. See the included LICENSE.md file for more details.
