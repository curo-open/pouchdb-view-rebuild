const PouchDB = require('pouchdb-node')
const fs = require('fs-extra')
const path = require('path')
require('chai').should()

const quickViewBuilder = require('../src/index')

let srcRoot = process.env.DB_SRC_PATH
let resRoot = process.env.DB_RESULT_ROOT

function copyDb(name) {
  let dbPath = path.join(resRoot, name)
  if (fs.existsSync(dbPath)) {
    fs.emptyDirSync(dbPath)
  }
  fs.copySync(srcRoot, dbPath)

  return new PouchDB(dbPath)
}

describe('quick parallel build of PouchDB views with leveldb driver', function () {
  let db, dbOld
  before(done => {
    if (!srcRoot || !fs.existsSync(srcRoot)) {
      throw 'missing source DB: ' + srcRoot
    }
    if (!resRoot) {
      throw 'missing value for DB_RESULT_ROOT'
    }
    done()
  })

  describe.skip('old DB manipulation', () => {
    before(done => {
      dbOld = copyDb('old')
      done()
    })


    it('rebuild old db views', async () => {
    })
  })

  describe('new DB manipulation', () => {
    before(done => {
      db = copyDb('new')
      done()
    })

    it('read views', async () => {
      let views = await quickViewBuilder(db, false)
      views.should.not.be.empty
    })

    it('build views in the fast way', async () => {try {quickViewBuilder(db, true) } catch (ex) {console.log(ex)}})
  })

  if (db && oldDb) {
    describe('compare view results')
  }
})
