#!/usr/bin/env node

import fse from 'fs-extra'
import path from 'path'
import dir from 'node-dir'
import express from 'express'
import session from 'express-session'
import cookieParser from 'cookie-parser'
import compression from 'compression'
import cors from 'cors'
import bodyParser from 'body-parser'
import passport from 'passport'
import passportLocal from 'passport-local'

import {dbPoolWorker, wrapForTransaction} from './src/dbPool'
import {search} from './src/search'
import * as iiif from './src/iiif'
import {getTags, updateTags} from './src/iiif/tags'
import * as iiifCollection from './src/iiif/collection'
import * as iiifManifest from './src/iiif/manifest'
import * as iiifRange from './src/iiif/range'
import * as iiifCanvas from './src/iiif/canvas'
import * as iiifTags from './src/iiif/tags'
import * as iiifValues from './src/iiif/values'
import * as buildings from './src/buildings'

const iiifTypeDescriptors = {
  'sc:Collection': {
    getOverrides: iiifCollection.getOverrides,
    saveOverrides: iiifCollection.setOverrides,
    getParents: iiifCollection.getParents,
  },
  'sc:Manifest': {
    getOverrides: iiifManifest.getOverrides,
    saveOverrides: iiifManifest.setOverrides,
    getParents: iiifManifest.getParents,
  },
  'sc:Range': {
    getOverrides: iiifRange.getOverrides,
    saveOverrides: iiifRange.setOverrides,
    getParents: iiifRange.getParents,
    dataExport: iiifRange.dataExport,
  },
  'sc:Canvas': {
    getOverrides: iiifCanvas.getOverrides,
    saveOverrides: iiifCanvas.setOverrides,
    getParents: iiifCanvas.getParents,
  },
}

const VHOST_SUFFIX = process.env.VHOST_SUFFIX
const jsonParser = bodyParser.json()
const urlParser = bodyParser.urlencoded()

export async function dbResPoolWorker(res, handler) {
  try {
    res.send(await dbPoolWorker(handler))
  } catch (e) {
    console.error(e)
    res.status(500).send('error')
  }
}

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n)
}

function isNotNeeded(value) {
  if (value === null || value === undefined || value === false) {
    return true
  }
  const valueType = typeof value
  if (valueType === 'string' || valueType === 'boolean' || isNumeric(value)) {
    return false
  }
  if (Array.isArray(value) && value.length === 0) {
    return true
  }
  if (Object.entries(value).filter(entry => !isNotNeeded(entry[1])).length === 0) {
    return true
  }
  return false
}

async function writeOneFile(targetName, fileContents) {
  const targetBaseName = path.basename(targetName)
  const targetDirName = path.dirname(targetName)
  await fse.mkdirs(targetDirName)
  await fse.writeFile(`${targetDirName}/${targetBaseName}.tmp`, fileContents)
  await fse.rename(`${targetDirName}/${targetBaseName}.tmp`, targetName)
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
    values: await iiifValues.getValues(client, iiifId),
    _overrides: await iiifTypeDescriptors[iiif_type_id].getOverrides(client, iiif_override_id)
  }
  const saveData = JSON.stringify(dataToSave, (key, value) => {
    return key === '' || !isNotNeeded(value) ? value : undefined
  }, 1)
  const targetName = '/srv/app/exports/' + escape(external_id) + '.iiif'
  await writeOneFile(targetName, saveData)
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
  const dataToSave = {notes, tags, values, ..._overrides}
  console.log('saving', external_id, iiif_id, dataToSave)
  await iiifValues.updateValues(iiif_id, values)
  return iiifTypeDescriptors[iiif_type_id].saveOverrides(client, iiif_id, dataToSave)
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
  await Promise.all(allOverridesResult.rows.map(row => saveOverridesToDisk(client, row.iiif_id)))
}

export async function findAllParents(client, initialType, initialSet) {
  const queue = [[initialType, initialSet]]
  const itemsByType = {}
  while (queue.length) {
    const [iiif_type_id, iiif_id] = queue.pop()
    const byType = itemsByType[iiif_type_id] || (itemsByType[iiif_type_id] = {})
    if (byType[iiif_id]) {
      continue
    }
    byType[iiif_id] = true
    const {[iiif_type_id]: {getParents} = {}} = iiifTypeDescriptors
    if (getParents) {
      queue.splice(-1, 0, ...(await getParents(client, iiif_id)))
    }
  }
  return Object.entries(itemsByType).reduce((result, entry) => {
    result[entry[0]] = Object.keys(entry[1]).map(id => parseInt(id))
    return result
  }, {})
}

async function exportOne(client, iiifId) {
  const iiifInfo = await client.query('SELECT a.iiif_type_id, a.external_id FROM iiif a WHERE a.iiif_id = $1', [iiifId])
  if (!iiifInfo.rows.length) {
    return
  }
  const {iiif_type_id, external_id} = iiifInfo.rows[0]
  const {[iiif_type_id]: {dataExport = id => null} = {}} = iiifTypeDescriptors
  const result = await dataExport(client, iiifId)
  if (!result) {
    return
  }
  const basePath = '/srv/app/geojson/' + escape(external_id)
  await Promise.all(Object.keys(result).map(async key => {
    const targetName = basePath + key
    const resultData = result[key]
    const fileContents = typeof resultData === 'string' ? resultData : JSON.stringify(resultData)
    await writeOneFile(targetName, fileContents)
  }))
}

async function exportAll(client) {
  const allResult = await client.query('SELECT a.iiif_id FROM iiif a')
  await Promise.all(allResult.rows.map(row => exportOne(client, row.iiif_id)))
}

async function exportData(client, parentInfo) {
  return Promise.all(Object.entries(parentInfo).map(async ([iiif_type_id, ids]) => {
    await Promise.all(ids.map(iiifId => exportOne(client, iiifId)))
  }))
}

const app = express()
app.use(cors({
  credentials: true,
  origin: `http://${VHOST_SUFFIX}`,
}))
app.use(compression({level: 9}))
app.use(cookieParser())
app.use(session({
  key: 'gis_session',
  secret: 'aaa5212b457b72f55125df8e9eb362d6',
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 24*60*60*1000,
  },
}))

class UserConfig {
  constructor(file) {
    this._file = file
  }

  express() {
    return async (req, res, next) => {
      const userDb = req.userDb = await userConfig.initialize()
      const {user} = req
      console.log('user', user)
      req.userPermissions = await userDb.getGroups(user)
      next()
    }
  }

  async initialize() {
    const {groups, users} = await fse.readJson(this._file)
    const {byId, byUsername} = users.reduce((users, user) => {
      users.byId[user.id] = user
      users.byUsername[user.username] = user
      return users
    }, {byId: {}, byUsername: {}})
    return new UserDb(groups, byId, byUsername)
  }

  async findById(id) {
    const userDb = await this.initialize()
    return userDb.findById(id)
  }

  async findByUsername(username) {
    const userDb = await this.initialize()
    return userDb.findByUsername(username)
  }
}

class UserDb {
  constructor(groups, byId, byUsername) {
    this._groups = groups
    this._byId = byId
    this._byUsername = byUsername
  }
  
  async findByUsername(username) {
    return this._byUsername[username]
  }

  async findById(id) {
    return this._byId[id]
  }

  async getGroups(user) {
    const groups = user && user.groups || ['anonymous']
    console.log('getGroups', user)
    return groups.reduce((permissions, group) => {
      const {[group]: groupPermissions = []} = this._groups
      console.log('groupPermissions', groupPermissions)
      groupPermissions.forEach(groupPermission => permissions[groupPermission] = true)
      return permissions
    }, {})
  }
}

const userConfig = new UserConfig('/srv/app/etc/users.js')

passport.use(new passportLocal.Strategy(async (username, password, done) => {
  const user = await userConfig.findByUsername(username)
  if (user) {
    if (user.password === password) {
      return done(null, user)
    } else {
      return done(null, false, {message: 'Invalid user or password'})
    }
  } else {
    return done(null, false, {message: 'Invalid user or password'})
  }
}))
passport.serializeUser((user, done) => {
  done(null, user.id)
})
passport.deserializeUser(async (id, done) => {
  const user = await userConfig.findById(id)
  console.log('deserializeUser', id, user)
  return done(null, user)
})

app.use(passport.initialize())
app.use(passport.session())
app.use(userConfig.express())
app.use((req, res, next) => {
  // auto-logout
  if (req.cookies.gis_session && !req.user) {
    //res.clearCookie('gis_session')
  }
  next()
})

const requirePermission = permission => {
  return (req, res, next) => {
    if (req.userPermissions) {
      if (req.userPermissions[permission]) {
        next()
      } else {
        res.status(500)
        res.send({type: 'permission-denied'})
      }
    } else {
      res.status(500)
      res.send({type: 'no-permissions'})
    }
  }
}

app.post('/user/logout', (req, res) => {
  req.logout()
  res.clearCookie('gis_session')
  res.redirect('/user/info')
})

function sendUserInfo(req, res) {
  const {user} = req
  const result = {
    isLoggedIn: user ? true : false,
    permissions: Object.keys(req.userPermissions),
  }

  if (user) {
    result.name = user.name
  }
  res.send(result)
}

app.post('/user/login', urlParser, passport.authenticate('local'), (req, res) => {
  res.redirect('/user/info')
})

app.get('/user/info', sendUserInfo)

app.post('/user/permissions', (req, res) => {
  res.send(Object.keys(req.userPermissions))
})

app.get('/search', async (req, res) => {
  const {address} = req.query
  dbResPoolWorker(res, client => search(client, {address}))
})

app.post('/_db/export-all', jsonParser, (req, res) => {
  dbResPoolWorker(res, client => exportAll(client))
})

app.post('/_db/load-all', jsonParser, (req, res) => {
  dbResPoolWorker(res, client => wrapForTransaction(loadAllOverrides)(client))
})

app.post('/_db/save-all', jsonParser, (req, res) => {
  dbResPoolWorker(res, client => saveAllOverrides(client))
})

app.post('/iiif/detectType', jsonParser, async (req, res) => {
  const {body: {iiifId, externalId}} = req
  dbResPoolWorker(res, client => iiif.detectType(client, {iiifId, externalId}))
})

app.get('/buildingsPlaced(/:rangeId)?', async (req, res) => {
  const {rangeId} = req.params
  dbResPoolWorker(res, client => buildings.getBuildingsPlaced(client, rangeId))
})

app.get('/buildings', async (req, res) => {
  const ids = Array.isArray(req.query.id) ? req.query.id : [req.query.id]
  dbResPoolWorker(res, client => buildings.getBuildings(client, ...ids))
})

app.get('/buildings/:buildingId/canvases', async (req, res) => {
  const {buildingId} = req.params
  dbResPoolWorker(res, client => buildings.getBuildingCanvases(client, buildingId))
})

app.get('/collection', async (req, res) => {
  dbResPoolWorker(res, client => iiifCollection.getAll(client))
})

app.get('/collection/:collectionId', (req, res) => {
  const {collectionId} = req.params
  dbResPoolWorker(res, async client => {
    const result = {}
    const base = await iiifCollection.getOne(client, collectionId)
    const values = await iiifValues.getValues(client, collectionId)
    return {...base, values}
  })
})

app.post('/collection/:collectionId', jsonParser, (req, res) => {
  const {collectionId} = req.params
  const {body: {notes, tags}} = req
  dbResPoolWorker(res, async client => {
    await iiifCollection.updateOne(client, collectionId, {notes, tags})
    await saveOverridesToDisk(client, collectionId)
    await exportData(client, await findAllParents(client, 'sc:Collection', collectionId))
  })
})

app.get('/manifest', (req, res) => {
	res.status(500).send('error')
})

app.get('/manifest/:manifestId', (req, res) => {
  const {manifestId} = req.params
  dbResPoolWorker(res, async client => {
    const result = {}
    const base = await iiifManifest.getOne(client, manifestId)
    const values = await iiifValues.getValues(client, manifestId)
    return {...base, values}
  })
})

app.post('/manifest/:manifestId', jsonParser, (req, res) => {
  const {manifestId} = req.params
  const {body: {notes, tags, values}} = req
  dbResPoolWorker(res, async client => {
    await iiifManifest.updateOne(client, manifestId, {notes, tags})
    await iiifValues.updateValues(client, manifestId, values)
    await saveOverridesToDisk(client, manifestId)
    await exportData(client, await findAllParents(client, 'sc:Manifest', manifestId))
  })
})

app.get('/manifest/:manifestId/structures', (req, res) => {
  const {manifestId} = req.params
  dbResPoolWorker(res, client => iiifManifest.getStructures(client, manifestId))
})

app.get('/range/:rangeId', (req, res) => {
  const {rangeId} = req.params
  dbResPoolWorker(res, async client => {
    const result = {}
    const base = await iiifRange.getOne(client, rangeId)
    const values = await iiifValues.getValues(client, rangeId)
    return {...base, values}
  })
})

app.post('/range/:rangeId', jsonParser, (req, res) => {
  const {rangeId} = req.params
  const {body: {notes, reverse, fovAngle, fovDepth, fovOrientation, tags}} = req
  dbResPoolWorker(res, async client => {
    await iiifRange.updateOne(client, rangeId, {notes, reverse, fovAngle, fovDepth, fovOrientation, tags})
    await saveOverridesToDisk(client, rangeId)
    await exportData(client, await findAllParents(client, 'sc:Range', rangeId))
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
  dbResPoolWorker(res, async client => {
    const result = {}
    const base = await iiifCanvas.getOne(client, canvasId)
    const values = await iiifValues.getValues(client, canvasId)
    return {...base, values}
  })
})

app.post('/canvas/:canvasId', jsonParser, (req, res) => {
  const {canvasId} = req.params
  const {body: {notes, tags = [], exclude = false, hole = false}} = req
  dbResPoolWorker(res, async client => {
    await iiifCanvas.updateOne(client, canvasId, {notes, exclude, hole, tags})
    await saveOverridesToDisk(client, canvasId)
    await exportData(client, await findAllParents(client, 'sc:Canvas', canvasId))
  })
})

app.post('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  const {canvasId, sourceId} = req.params
  const {body: {priority, point}} = req
  dbResPoolWorker(res, async client => {
    await iiifCanvas.point.updateOne(client, canvasId, sourceId, {priority, point})
    await saveOverridesToDisk(client, canvasId)
    await exportData(client, await findAllParents(client, 'sc:Canvas', canvasId))
  })
})

app.delete('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  const {canvasId, sourceId} = req.params
  dbResPoolWorker(res, async client => {
    await iiifCanvas.point.deleteOne(client, canvasId, sourceId)
    await saveOverridesToDisk(client, canvasId)
    await exportData(client, await findAllParents(client, 'sc:Canvas', canvasId))
  })
})

app.post('/edge/by-point', jsonParser, (req, res) => {
  const {body: {point}} = req
  dbResPoolWorker(res, client => {
    return iiifCanvas.point.nearestEdge(client, point)
  })
})

app.post('/stats/range', jsonParser, (req, res) => {
  dbResPoolWorker(res, async client => {
    const claimedRanges = await iiifTags.searchTags(client, {types: ['sc:Range'], tags: ['Claimed']})
    const placedRanges = await iiifTags.searchTags(client, {types: ['sc:Range'], tags: ['Placed']})
    const validatedRanges = await iiifTags.searchTags(client, {types: ['sc:Range'], tags: ['Validated']})
    return {
      claimed: claimedRanges.length,
      placed: placedRanges.length,
      validated: validatedRanges.length,
    }
  })
})

app.listen(8080)
