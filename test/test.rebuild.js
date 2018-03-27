const PouchDB = require('pouchdb-node')
const fs = require('fs-extra')
const path = require('path')
const testRoot = process.env.DB_RESULT_ROOT || '/tmp/pdb-test'
require('chai').should()

const quickViewBuilder = require('../src/index')

let sampleDocs = {
  docs: [
    { name: 'foo' },
    { name: 'bar' },
    { name: 'baz' },
  ]
}

let views = [
  {
    _id: '_design/index1',
    views: {
      'index1': {
        map: function (doc) {
          emit(doc.name)
        }
      }
    }
  },
  {
    _id: '_design/index2',
    views: {
      'index2_a': {
        map: function (doc) {
          emit(doc.name)
        }
      },
      'index2_b': {
        map: function (doc) {
          emit(doc.name)
        }
      }
    }
  }
]

describe('quick parallel build of PouchDB views with leveldb driver', async () => {
  let db, dbOld
  before(done => {
    if (fs.existsSync(testRoot)) {
      fs.emptyDirSync(testRoot)
    }
    fs.ensureDirSync(testRoot)
    db = new PouchDB(path.join(testRoot, 'new'))
    dbOld = new PouchDB(path.join(testRoot, 'old'))
    done()
  })

  it('create PouchDb with some documents', async () => {
    await db.bulkDocs(sampleDocs)
    await dbOld.bulkDocs(sampleDocs)

    let viewsStr = []
    for (let d of views) {
      let index = {_id: d._id, views: {}}
      for (let v in d.views) {
        if (!d.views.hasOwnProperty(v)) continue
        index.views[v] = index.views[v] || {}
        index.views[v].map = d.views[v].map.toString()
      }
      viewsStr.push(index)
    }
    await dbOld.bulkDocs(viewsStr)
    await dbOld.query('index1')
  })

  it('build views in the fast way', async () => quickViewBuilder(db, views))

  it('views are recognized by PouchDb', async () => {
    let oldR = await dbOld.allDocs({
      include_docs: true,
      // attachments: true,
      startkey: '_design/',
      endkey: '_design/' + '\uffff'
    })

    let quickR = await db.allDocs({
      include_docs: true,
      // attachments: true,
      startkey: '_design/',
      endkey: '_design/' + '\uffff'
    })
    quickR.rows.length.should.equal(oldR.rows.length)
  })

  it('views return right results', async () => {
    let oldR = await dbOld.query('index1')
    let quickR = await db.query('index1')
    quickR.rows.length.should.equal(oldR.rows.length)
    quickR.rows[0].key.should.equal(oldR.rows[0].key)
    quickR.rows[1].key.should.equal(oldR.rows[1].key)
    quickR.rows[2].key.should.equal(oldR.rows[2].key)
  })
})
