const fs = require('fs-extra')
const path = require('path')
const crypto = require('crypto')
const temporary = false // only to keep depDbName similar as possible to original
const debug = require('debug')('pouchdb:build-view')

let localDocName = 'mrviews'
let MIN_MAGNITUDE = -324 // verified by -Number.MIN_VALUE
let MAGNITUDE_DIGITS = 3 // ditto
let SEP = '' // set to '_' for easier debugging

class Builder {
  constructor (db, views, opts) {
    this.db = db
    this.views = views
    this.chunkSizeRead = opts.chunkSizeRead || 200
    this.chunkSizeWrite = opts.chunkSizeWrite || 50
    this.forceRebuild = opts.forceRebuild || false
  }

  async build () {
    // clean up views if needed
    if (this.forceRebuild) {
      await this.dropViews(drop)
    }

    let [views, viewsStr] = this._normalizeViews(this.views)

    // put design documents
    try {
      await Promise.all(viewsStr.map(v => this.upsert(this.db, v)))
    } catch (err) {
      throw new Error(`Error when putting views: ${err}`)
    }

    let targets = []
    let depViews = {}
    let shouldExists = new Set()
    for (let i = 0; i < viewsStr.length; i++) {
      let idx = viewsStr[i]
      for (let viewName in idx.views) {
        if (!idx.views.hasOwnProperty(viewName)) {
          continue
        }
        let v = idx.views[viewName]

        // generate dependent db name
        let depDbName = await this.getDepDbName(v)

        // gather dependent views
        let fullViewName = viewName
        if (fullViewName.indexOf('/') === -1) {
          fullViewName = viewName + '/' + viewName
        }
        depViews[fullViewName] = { [depDbName]: true }

        if (!this.forceRebuild) {
          // remeber that it should not be cleaned later
          shouldExists.add(path.basename(depDbName))
          // decide if needs to be rebuilded
          if (fs.existsSync(depDbName)) {
            // no need to rebuild this view
            continue
          }
        } else {
          // remove dependent db if already exists
          fs.removeSync(depDbName)
        }

        // register dependent db
        let res$$1 = await this.db.registerDependentDatabase(depDbName)
        v.db = res$$1.db
        v.db.auto_compaction = true
        v.map = views[i].views[viewName].map
        targets.push(v)
      }
    }

    if (targets.length) {
      // put dependent view doc
      try {
        await this.upsert(this.db, { _id: '_local/' + localDocName, views: depViews })
      } catch (err) {
        throw new Error(`Error when putting dependent views doc: ${err}`)
      }

      // get all IDs
      debug('get all IDs to process')
      let data = await this.db.allDocs({})
      let j = data.rows.length
      debug('%d docs ready', j)

      // receive real documents in reasonable amount, not to overload memory
      for (let i = 0; i < j; i += this.chunkSizeRead) {
        let subset = data.rows.slice(i, i + this.chunkSizeRead).map(d => d.id)
        let docs = await this.db.allDocs({
          include_docs: true,
          keys: subset
        })

        // store the data before next chunk will be loaded
        let jj = docs.rows.length
        for (let ii = 0; ii < jj; ii += this.chunkSizeWrite) {
          // clear targets
          targets.forEach(t => {
            t.docs = []
          })

          for (let d of docs.rows.slice(ii, ii + this.chunkSizeWrite)) {
            // ignore some records
            if (d.error || d.deleted || d.doc._id[0] === '_') continue

            // evaluate views
            for (let t of targets) {
              // evaluate map
              mapResults = []
              doc = d.doc
              try {
                t.map(doc, emit)
              } catch (err) {
                // TODO
                throw err
              }

              // TODO: evaluate reduce

              // get documents
              let docsToPersist = await this.getDocsToPersist(doc._id, t, this.createIndexableKeysToKeyValues(mapResults))

              if (docsToPersist.length) {
                t.docs = t.docs.concat(docsToPersist) // accumulate for writes
              }
            }
          }

          // construct array of promises
          let writes = targets
            .filter(t => t.docs.length)
            .map(t => t.db.bulkDocs({ docs: t.docs }))

          // write to all views
          if (writes.length) {
            await Promise.all(writes)
          }
        }
      }

      // write _local/lastSeq
      let changes
      try {
        changes = await this.db.changes({
          conflicts: true,
          since: 0,
          limit: 1,
          descending: true
        })
      } catch (err) {
        console.error('Error when calculating changes')
        changes = {}
      }
      let lastSeq = changes.last_seq || j
      await Promise.all(targets.map(t => t.db.bulkDocs({ docs: [{ _id: '_local/lastSeq', seq: lastSeq }] })))

      // close all view dbs
      for (let t of targets) {
        try {
          await t.db.close()
        } catch (err) {}
      }
    } else {
      debug('no view needs rebuild')
    }

    let prefix = path.basename(this.db.name) + '-mrview-'
    let dbpath = path.dirname(this.db.name)
    debug('removing view files that are invalid and starting with prefix %s', prefix)
    for (let name of fs.readdirSync(dbpath)) {
      if (name.startsWith(prefix)) {
        if (!shouldExists.has(name)) {
          let fn = path.join(dbpath, name)
          debug('leftover view: %s', fn)
          fs.removeSync(fn)
        }
      }
    }

    debug('view build finished')
  }

  _normalizeViews (def) {
    let views = []
    let viewsStr = []
    for (let d of def) {
      let index = { _id: d._id, views: {} }
      let indexStr = { _id: d._id, views: {} }
      for (let v in d.views) {
        if (!d.views.hasOwnProperty(v)) continue
        index.views[v] = index.views[v] || {}
        indexStr.views[v] = indexStr.views[v] || {}
        index.views[v].map = this._functify(d.views[v].map)
        indexStr.views[v].map = this._stringify(d.views[v].map)
        if (d.views[v].reduce) {
          index.views[v].reduce = this._functify(d.views[v].reduce)
          indexStr.views[v].reduce = this._stringify(d.views[v].reduce)
        }
      }
      views.push(index)
      viewsStr.push(indexStr)
    }
    return [views, viewsStr]
  }

  async dropViews () {
    let res = await this.db.allDocs({
      include_docs: true,
      // attachments: true,
      startkey: '_design/',
      endkey: '_design/' + '\uffff'
    })
    for (let row of res.rows) {
      await this.db.remove(row.doc)
      for (let viewName in row.doc.views) {
        if (!row.doc.views.hasOwnProperty(viewName)) continue
        let v = row.doc.views[viewName]
        // generate dependent db name
        let depDbName = await this.getDepDbName(v)
        // remove dependent db if already exists
        fs.removeSync(depDbName)
      }
    }
  }

  async upsert (db, doc) {
    try {
      let oldDoc = await db.get(doc._id)
      doc._rev = oldDoc._rev
    } catch (err) {
      if (err.status !== 404) {
        throw err
      }
    }
    return db.put(doc)
  }

  async getDepDbName (v) {
    return this.db.name + '-mrview-' + (temporary ? 'temp' : this.stringMd5(this.createViewSignature(v.map, v.reduce)))
  }

  /** FOLLOWING FUNCTIONS ARE COPIED FROM POUCHDB SOURCE **/
  stringify (input) {
    if (!input) {
      return 'undefined' // backwards compat for empty reduce
    }
    // for backwards compat with mapreduce, functions/strings are stringified
    // as-is. everything else is JSON-stringified.
    switch (typeof input) {
      case 'function':
        // e.g. a mapreduce map
        return input.toString()
      case 'string':
        // e.g. a mapreduce built-in _reduce function
        return input.toString()
      default:
        // e.g. a JSON object in the case of mango queries
        return JSON.stringify(input)
    }
  }

  createViewSignature (mapFun, reduceFun) {
    return this.stringify(mapFun) + this.stringify(reduceFun) + 'undefined'
  }

  stringMd5 (string) {
    return crypto.createHash('md5').update(string, 'binary').digest('hex')
  }

  getDocsToPersist (docId, view, indexableKeysToKeyValues) {
    let metaDocId = '_local/doc_' + docId
    let defaultMetaDoc = { _id: metaDocId, keys: [] }

    let processKeyValueDocs = (metaDoc) => {
      let kvDocs = []
      let newKeys = this.mapToKeysArray(indexableKeysToKeyValues)
      newKeys.forEach(function (key) {
        // new doc
        let kvDoc = {
          _id: key
        }
        let keyValue = indexableKeysToKeyValues.get(key)
        if ('value' in keyValue) {
          kvDoc.value = keyValue.value
        }
        kvDocs.push(kvDoc)
      })
      metaDoc.keys = this.uniq(newKeys)
      kvDocs.push(metaDoc)
      return kvDocs
    }

    return Promise.resolve(processKeyValueDocs(defaultMetaDoc))
  }

  mapToKeysArray (map) {
    let result = new Array(map.size)
    let index = -1
    map.forEach(function (value, key) {
      result[++index] = key
    })
    return result
  }

  uniq (arr) {
    let theSet = new Set(arr)
    let result = new Array(theSet.size)
    let index = -1
    theSet.forEach(function (value) {
      result[++index] = value
    })
    return result
  }

  createIndexableKeysToKeyValues (mapResults) {
    let indexableKeysToKeyValues = new Map()
    let lastKey
    for (let i = 0, len = mapResults.length; i < len; i++) {
      let emittedKeyValue = mapResults[i]
      let complexKey = [emittedKeyValue.key, emittedKeyValue.id]
      if (i > 0 && this.collate(emittedKeyValue.key, lastKey) === 0) {
        complexKey.push(i) // dup key+id, so make it unique
      }
      indexableKeysToKeyValues.set(this.toIndexableString(complexKey), emittedKeyValue)
      lastKey = emittedKeyValue.key
    }
    return indexableKeysToKeyValues
  }

  indexify (key) {
    if (key !== null) {
      switch (typeof key) {
        case 'boolean':
          return key ? 1 : 0
        case 'number':
          return this.numToIndexableString(key)
        case 'string':
          // We've to be sure that key does not contain \u0000
          // Do order-preserving replacements:
          // 0 -> 1, 1
          // 1 -> 1, 2
          // 2 -> 2, 2
          return key
            .replace(/\u0002/g, '\u0002\u0002')
            .replace(/\u0001/g, '\u0001\u0002')
            .replace(/\u0000/g, '\u0001\u0001')
        case 'object':
          let isArray = Array.isArray(key)
          let arr = isArray ? key : Object.keys(key)
          let i = -1
          let len = arr.length
          let result = ''
          if (isArray) {
            while (++i < len) {
              result += this.toIndexableString(arr[i])
            }
          } else {
            while (++i < len) {
              let objKey = arr[i]
              result += this.toIndexableString(objKey) +
                this.toIndexableString(key[objKey])
            }
          }
          return result
      }
    }
    return ''
  }

  toIndexableString (key) {
    let zero = '\u0000'
    key = this.normalizeKey(key)
    return this.collationIndex(key) + SEP + this.indexify(key) + zero
  }

  numToIndexableString (num) {
    if (num === 0) {
      return '1'
    }

    // convert number to exponential format for easier and
    // more succinct string sorting
    let expFormat = num.toExponential().split(/e\+?/)
    let magnitude = parseInt(expFormat[1], 10)

    let neg = num < 0

    let result = neg ? '0' : '2'

    // first sort by magnitude
    // it's easier if all magnitudes are positive
    let magForComparison = ((neg ? -magnitude : magnitude) - MIN_MAGNITUDE)
    let magString = this.padLeft((magForComparison).toString(), '0', MAGNITUDE_DIGITS)

    result += SEP + magString

    // then sort by the factor
    let factor = Math.abs(parseFloat(expFormat[0])) // [1..10)
    /* istanbul ignore next */
    if (neg) { // for negative reverse ordering
      factor = 10 - factor
    }

    let factorStr = factor.toFixed(20)

    // strip zeros from the end
    factorStr = factorStr.replace(/\.?0+$/, '')

    result += SEP + factorStr

    return result
  }

  pad (str, padWith, upToLength) {
    let padding = ''
    let targetLength = upToLength - str.length
    /* istanbul ignore next */
    while (padding.length < targetLength) {
      padding += padWith
    }
    return padding
  }

  padLeft (str, padWith, upToLength) {
    let padding = this.pad(str, padWith, upToLength)
    return padding + str
  }

  normalizeKey (key) {
    switch (typeof key) {
      case 'undefined':
        return null
      case 'number':
        if (key === Infinity || key === -Infinity || isNaN(key)) {
          return null
        }
        return key
      case 'object':
        let origKey = key
        if (Array.isArray(key)) {
          let len = key.length
          key = new Array(len)
          for (let i = 0; i < len; i++) {
            key[i] = this.normalizeKey(origKey[i])
          }
          /* istanbul ignore next */
        } else if (key instanceof Date) {
          return key.toJSON()
        } else if (key !== null) { // generic object
          key = {}
          for (let k in origKey) {
            if (origKey.hasOwnProperty(k)) {
              let val = origKey[k]
              if (typeof val !== 'undefined') {
                key[k] = this.normalizeKey(val)
              }
            }
          }
        }
    }
    return key
  }

  collate (a, b) {
    if (a === b) {
      return 0
    }

    a = this.normalizeKey(a)
    b = this.normalizeKey(b)

    let ai = this.collationIndex(a)
    let bi = this.collationIndex(b)
    if ((ai - bi) !== 0) {
      return ai - bi
    }
    switch (typeof a) {
      case 'number':
        return a - b
      case 'boolean':
        return a < b ? -1 : 1
      case 'string':
        return this.stringCollate(a, b)
    }
    return Array.isArray(a) ? this.arrayCollate(a, b) : this.objectCollate(a, b)
  }

  arrayCollate (a, b) {
    let len = Math.min(a.length, b.length)
    for (let i = 0; i < len; i++) {
      let sort = this.collate(a[i], b[i])
      if (sort !== 0) {
        return sort
      }
    }
    return (a.length === b.length) ? 0 :
      (a.length > b.length) ? 1 : -1
  }

  stringCollate (a, b) {
    // See: https://github.com/daleharvey/pouchdb/issues/40
    // This is incompatible with the CouchDB implementation, but its the
    // best we can do for now
    return (a === b) ? 0 : ((a > b) ? 1 : -1)
  }

  objectCollate (a, b) {
    let ak = Object.keys(a)
    let bk = Object.keys(b)
    let len = Math.min(ak.length, bk.length)
    for (let i = 0; i < len; i++) {
      // First sort the keys
      let sort = this.collate(ak[i], bk[i])
      if (sort !== 0) {
        return sort
      }
      // if the keys are equal sort the values
      sort = this.collate(a[ak[i]], b[bk[i]])
      if (sort !== 0) {
        return sort
      }
    }
    return (ak.length === bk.length) ? 0 :
      (ak.length > bk.length) ? 1 : -1
  }

  collationIndex (x) {
    let id = ['boolean', 'number', 'string', 'object']
    let idx = id.indexOf(typeof x)
    // false if -1 otherwise true, but fast!!!!1
    if (~idx) {
      if (x === null) {
        return 1
      }
      if (Array.isArray(x)) {
        return 5
      }
      return idx < 3 ? (idx + 2) : (idx + 3)
    }
    /* istanbul ignore next */
    if (Array.isArray(x)) {
      return 5
    }
  }

  _functify (str) {
    // let fn = '/tmp/x.js'
    // fs.writeFileSync(fn, 'module.exports = ' + str)
    // let func= require(fn)
    return typeof str === 'string' ? new Function('doc', str.substr(str.indexOf('{'))) : str
  }

  _stringify (fun) {
    return fun.toString()
  }

  async getCurrentViewDefinitions () {
    let info = await this.db.info()
    let res = await this.db.allDocs({
      include_docs: true,
      // attachments: true,
      startkey: '_design/',
      endkey: '_design/' + '\uffff'
    })
    let views = []
    res.rows.filter(r => {
      for (let v of Object.values(r.doc.views)) {
        let viewFolderName = info.db_name + '-mrview-' + this.stringMd5(this.createViewSignature(v.map, v.reduce))
        console.log(viewFolderName)
        v._folder = viewFolderName
      }
      views.push({ _id: r.id, views: r.doc.views })
    })

    return views
  }

  static getLoopbackViewDefinitions (def) {
    let views = []
    for (let idx in def) {
      if (!def.hasOwnProperty(idx)) continue
      views.push({ _id: `_design/${idx}`, views: def[idx].views })
    }
    return views
  }
}

let doc
let mapResults
emit = function (key, value) {
  let output = { id: doc._id, key: b.normalizeKey(key) }
  // Don't explicitly store the value unless it's defined and non-null.
  // This saves on storage space, because often people don't use it.
  if (typeof value !== 'undefined' && value !== null) {
    output.value = b.normalizeKey(value)
  }
  mapResults.push(output)
}

let b

async function Main (db, views = true, opts = { chunkSizeRead: 200, chunkSizeWrite: 50, forceRebuild: false }) {
  b = new Builder(db, views, opts)
  if (views === false) {
    // only return views
    return b.getCurrentViewDefinitions()
  } else if (views === true) {
    // rebuild views defined in database
    b.views = await b.getCurrentViewDefinitions()
  } else if (!Array.isArray(views)) {
    // this is loopback structure
    b.views = await Builder.getLoopbackViewDefinitions(views)
  } else {
    b.views = views
  }
  return b.build()
}

Main.getLoopbackViewDefinitions = Builder.getLoopbackViewDefinitions

// ddocValidator
// checkQueryParseError
// !!! createView
// !!! updateViewInQueue
// !!! registerDependentDatabase
// processBatch

module.exports = Main
