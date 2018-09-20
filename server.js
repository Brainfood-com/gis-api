#!/usr/bin/env node

import fse from 'fs-extra'
import path from 'path'
import dir from 'node-dir'
import express from 'express'
import cors from 'cors'
import bodyParser from 'body-parser'
import { Client, Pool } from 'pg'

import {getTags, updateTags} from './src/iiif/tags'
import * as iiifCollection from './src/iiif/collection'
import * as iiifManifest from './src/iiif/manifest'
import * as iiifRange from './src/iiif/range'
import * as iiifCanvas from './src/iiif/canvas'
import * as buildings from './src/buildings'

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

export async function dbPoolWorker(handler) {
  const client = await pool.connect()
  try {
    return await handler(client)
  } finally {
    client.release()
  }
}

export async function dbResPoolWorker(res, handler) {
  try {
    res.send(await dbPoolWorker(handler))
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
    tags: await getTags(client, iiifId),
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
  dbResPoolWorker(res, client => loadAllOverrides(client))
})

app.post('/_db/save-all', jsonParser, (req, res) => {
  dbResPoolWorker(res, client => saveAllOverrides(client))
})

app.get('/buildings', async (req, res) => {
  const ids = Array.isArray(req.query.id) ? req.query.id : []
  dbResPoolWorker(res, client => buildings.getBuildings(client, ...ids))
})

app.get('/collection', async (req, res) => {
  dbResPoolWorker(res, client => iiifCollection.getAll(client))
})

app.get('/collection/:collectionId', (req, res) => {
  const {collectionId} = req.params
  dbResPoolWorker(res, client => iiifCollection.getOne(client, collectionId))
})

app.post('/collection/:collectionId', jsonParser, (req, res) => {
  const {collectionId} = req.params
  const {body: {notes, tags}} = req
  dbResPoolWorker(res, client => {
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
  dbResPoolWorker(res, client => iiifManifest.getOne(client, manifestId))
})

app.post('/manifest/:manifestId', jsonParser, (req, res) => {
  const {manifestId} = req.params
  const {body: {notes, tags}} = req
  dbResPoolWorker(res, client => {
    return iiifManifest.updateOne(client, manifestId, {notes, tags}).then(result => {
      saveOverridesToDisk(client, manifestId)
    })
  })
})

app.get('/manifest/:manifestId/structures', (req, res) => {
  const {manifestId} = req.params
  dbResPoolWorker(res, client => iiifManifest.getStructures(client, manifestId))
})

app.get('/range/:rangeId', (req, res) => {
  const {rangeId} = req.params
  dbResPoolWorker(res, client => iiifRange.getOne(client, rangeId))
})

app.post('/range/:rangeId', jsonParser, (req, res) => {
  const {rangeId} = req.params
  const {body: {notes, fovAngle, fovDepth, fovOrientation, tags}} = req
  dbResPoolWorker(res, client => {
    return iiifRange.updateOne(client, rangeId, {notes, fovAngle, fovDepth, fovOrientation, tags}).then(result => {
      saveOverridesToDisk(client, rangeId)
    })
  })
})

app.get('/range/:rangeId/canvasPoints', (req, res) => {
  const {rangeId} = req.params
  dbResPoolWorker(res, client => iiifRange.getCanvasPoints(client, rangeId))
})

app.get('/range/:rangeId/geoJSON', (req, res) => {
  const {rangeId} = req.params
  dbResPoolWorker(res, client => iiifRange.getGeoJSON(client, rangeId))
})

app.get('/canvas/:canvasId', (req, res) => {
  const {canvasId} = req.params
  dbResPoolWorker(res, client => iiifCanvas.getOne(client, canvasId))
})

app.post('/canvas/:canvasId', jsonParser, (req, res) => {
  const {canvasId} = req.params
  const {body: {notes, tags = [], exclude = false, hole = false}} = req
  dbResPoolWorker(res, client => {
    return iiifCanvas.updateOne(client, canvasId, {notes, exclude, hole, tags}).then(result => {
      saveOverridesToDisk(client, canvasId)
    })
  })
})

app.post('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  const {canvasId, sourceId} = req.params
  const {body: {priority, point}} = req
  dbResPoolWorker(res, client => {
    return iiifCanvas.point.updateOne(client, canvasId, sourceId, {priority, point}).then(result => {
      saveOverridesToDisk(client, canvasId)
    })
  })
})

app.delete('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  const {canvasId, sourceId} = req.params
  dbResPoolWorker(res, client => {
    return iiifCanvas.point.deleteOne(client, canvasId, sourceId).then(result => {
      saveOverridesToDisk(client, canvasId)
    })
  })
})

app.listen(8080)
