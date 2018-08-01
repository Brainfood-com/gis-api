#!/usr/bin/env node

const fse = require('fs-extra')
const path = require('path')
const dir = require('node-dir')
const express = require('express')
const cors = require('cors')
const bodyParser = require('body-parser')
const { Client, Pool } = require('pg')

const iiifTags = require('./src/iiif/tags')
const iiifCollection = require('./src/iiif/collection')
const iiifManifest = require('./src/iiif/manifest')
const iiifRange = require('./src/iiif/range')
const iiifCanvas = require('./src/iiif/canvas')

const getOverrides = {
  'sc:Collection': iiifCollection.getOverrides,
  'sc:Manifest': iiifManifest.getOverrides,
  'sc:Range': iiifRange.getOverrides,
  'sc:Canvas': iiifCanvas.getOverrides,
}
const saveOverrides = {
  'sc:Collection': iiifCollection.setOverrides,
  'sc:Manifest': iiifManifest.setOverrides,
  'sc:Range': iiifRange.setOverrides,
  'sc:Canvas': iiifCanvas.setOverrides,
}

const jsonParser = bodyParser.json()
const dbConf = {
	user: 'gis',
	host: 'postgresql',
	database: 'gis',
	password: 'sig',
}
const pool = new Pool({
  ...dbConf,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
})

//    this._pgConnection = new Client({

async function dbPoolWorker(res, handler) {
  try {
    const client = await pool.connect()
    try {
      res.send(await handler(client))
    } finally {
      client.release()
    }
  } catch (e) {
    console.error(e)
    res.status(500).send('error')
  }
}

async function saveOverridesToDisk(client, iiifId) {
  const iiifInfo = await client.query('SELECT b.iiif_override_id, a.external_id, a.iiif_type_id, b.notes FROM iiif a JOIN iiif_overrides b ON a.external_id = b.external_id WHERE a.iiif_id = $1', [iiifId])
  if (iiifInfo.rows.length === 0) {
    return null
  }
  const {iiif_override_id, iiif_type_id, external_id, notes} = iiifInfo.rows[0]
  const dataToSave = {
    _version: '1',
    external_id,
    iiif_type_id,
    notes,
    tags: await iiifTags.getOne(client, iiifId),
    _overrides: await getOverrides[iiif_type_id](client, iiif_override_id)
  }
  console.log('saveData', JSON.stringify(dataToSave, null, 1))
  const targetName = '/srv/app/exports/' + escape(external_id) + '.iiif'
  const targetBaseName = path.basename(targetName)
  const targetDirName = path.dirname(targetName)
  await fse.mkdirs(targetDirName)
  await fse.writeFile(`${targetDirName}/${targetBaseName}.tmp`, JSON.stringify(dataToSave, null, 1))
  await fse.rename(`${targetDirName}/${targetBaseName}.tmp`, targetName)
}

async function loadOverrideFromDisk(client, file) {
  const data = await fse.readJson(file)
  // FIXME: Deal with _version
  const {_version, external_id, iiif_type_id, notes, tags, _overrides} = data
  const iiifInfo = await client.query('SELECT a.iiif_id, a.external_id, a.iiif_type_id FROM iiif a WHERE a.external_id = $1', [external_id])
  const firstRow = iiifInfo.rows[0] || {}
  if (firstRow.iiif_type_id !== iiif_type_id && firstRow.external_id !== external_id) {
    return
  }
  const {iiif_id} = firstRow
  const dataToSave = {notes, tags, ..._overrides}
  console.log('saving', external_id, iiif_id, dataToSave)
  return saveOverrides[iiif_type_id](client, iiif_id, dataToSave)
}

async function loadAllOverrides(client) {
  const iiifFiles = (await dir.promiseFiles('/srv/app/exports')).filter(file => file.match(/\.iiif$/))
  for (const file of iiifFiles) {
    await loadOverrideFromDisk(client, file)
  }
  return {count: iiifFiles.length}
}

async function saveAllOverrides(client) {
  const allOverridesResult = await client.query('SELECT a.iiif_id FROM iiif a JOIN iiif_overrides b ON a.external_id = b.external_id')
  await allOverridesResult.rows.map(row => saveOverridesToDisk(client, row.iiif_id))
}

const app = express()
app.use(cors())

app.post('/_db/load-all', jsonParser, (req, res) => {
  dbPoolWorker(res, client => loadAllOverrides(client))
})

app.post('/_db/save-all', jsonParser, (req, res) => {
  dbPoolWorker(res, client => saveAllOverrides(client))
})

app.get('/collection', async (req, res) => {
  dbPoolWorker(res, client => iiifCollection.getAll(client))
})

app.get('/collection/:collectionId', (req, res) => {
  const {collectionId} = req.params
  dbPoolWorker(res, client => iiifCollection.getOne(client, collectionId))
})

app.post('/collection/:collectionId', jsonParser, (req, res) => {
  const {collectionId} = req.params
  const {body: {notes, tags}} = req
  dbPoolWorker(res, client => {
    return iiifCollection.updateOne(client, collectionId, {notes, tags}).then(result => {
      saveOverridesToDisk(client, collectionId)
    })
  })
})

app.get('/manifest', (req, res) => {
	res.status(500).send('error')
})

app.get('/manifest/:manifestId', (req, res) => {
  const {manifestId} = req.params
  dbPoolWorker(res, client => iiifManifest.getOne(client, manifestId))
})

app.post('/manifest/:manifestId', jsonParser, (req, res) => {
  const {manifestId} = req.params
  const {body: {notes, tags}} = req
  dbPoolWorker(res, client => {
    return iiifManifest.updateOne(client, manifestId, {notes, tags}).then(result => {
      saveOverridesToDisk(client, manifestId)
    })
  })
})

app.get('/manifest/:manifestId/structures', (req, res) => {
  const {manifestId} = req.params
  dbPoolWorker(res, client => iiifManifest.getStructures(client, manifestId))
})

app.get('/range/:rangeId', (req, res) => {
  const {rangeId} = req.params
  dbPoolWorker(res, client => iiifRange.getOne(client, rangeId))
})

app.post('/range/:rangeId', jsonParser, (req, res) => {
  const {rangeId} = req.params
  const {body: {notes, fovAngle, fovDepth, fovOrientation, tags}} = req
  dbPoolWorker(res, client => {
    return iiifRange.updateOne(client, rangeId, {notes, fovAngle, fovDepth, fovOrientation, tags}).then(result => {
      saveOverridesToDisk(client, rangeId)
    })
  })
})

app.get('/range/:rangeId/canvasPoints', (req, res) => {
  const {rangeId} = req.params
  dbPoolWorker(res, client => iiifRange.getCanvasPoints(client, rangeId))
})

app.get('/canvas/:canvasId', (req, res) => {
  const {canvasId} = req.params
  dbPoolWorker(res, client => iiifCanvas.getOne(client, canvasId))
})

app.post('/canvas/:canvasId', jsonParser, (req, res) => {
  const {canvasId} = req.params
  const {body: {notes, tags = [], exclude = false, hole = false}} = req
  dbPoolWorker(res, client => {
    return iiifCanvas.updateOne(client, canvasId, {notes, exclude, hole, tags}).then(result => {
      saveOverridesToDisk(client, canvasId)
    })
  })
})

app.post('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  const {canvasId, sourceId} = req.params
  const {body: {priority, point}} = req
  dbPoolWorker(res, client => {
    return iiifCanvas.point.updateOne(client, canvasId, sourceId, {priority, point}).then(result => {
      saveOverridesToDisk(client, canvasId)
    })
  })
})

app.delete('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  const {canvasId, sourceId} = req.params
  dbPoolWorker(res, client => {
    return iiifCanvas.point.deleteOne(client, canvasId, sourceId).then(result => {
      saveOverridesToDisk(client, canvasId)
    })
  })
})

app.listen(8080)
