#!/usr/bin/env node

const express = require('express')
const cors = require('cors')
const { Client, Pool } = require('pg')

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

const app = express()
app.use(cors())

app.get('/collection', async (req, res) => {
  dbPoolWorker(res, async client => {
    const collectionResult = await client.query("SELECT iiif_id, label FROM iiif WHERE iiif_type_id = 'sc:Collection'")
    return collectionResult.rows.map(row => {
      return {id: row.iiif_id, label: row.label}
    })
  })
})

app.get('/collection/:collectionId', (req, res) => {
  dbPoolWorker(res, async client => {
    const {collectionId} = req.params
    const collectionResult = await client.query("SELECT iiif_id, label FROM iiif WHERE iiif_type_id = 'sc:Collection' AND iiif_id = $1", [collectionId])
    const assocResult = await client.query("SELECT a.iiif_id_to, a.iiif_assoc_type_id, b.label FROM iiif_assoc a JOIN iiif b ON a.iiif_id_to = b.iiif_id WHERE a.iiif_id_from = $1 ORDER BY a.sequence_num, b.label", [collectionId])
    return result = {
      id: collectionId,
      label: collectionResult.rows[0].label,
      members: assocResult.rows.map(row => {
        return {id: row.iiif_id_to, type: row.iiif_assoc_type_id, label: row.label}
      })
    }
  })
})

app.get('/manifest', (req, res) => {
	res.status(500).send('error')
})

app.get('/manifest/:manifestId', (req, res) => {
  dbPoolWorker(res, async client => {
    const {manifestId} = req.params
    const manifestResult = await client.query("SELECT a.label, b.* FROM iiif a JOIN iiif_manifest b ON a.iiif_id = b.iiif_id WHERE a.iiif_id = $1", [manifestId])
    const manifestRangesResult = await client.query("SELECT a.iiif_id_to AS range_id, b.label, c.viewing_hint FROM iiif_assoc a JOIN iiif b ON a.iiif_id_to = b.iiif_id AND a.iiif_assoc_type_id = 'sc:Range' JOIN iiif_range c ON b.iiif_id = c.iiif_id WHERE a.iiif_id_from = $1 ORDER BY a.sequence_num, b.label", [manifestId])
    const manifestRangeMembersResult = await client.query("SELECT a.iiif_id_to AS range_id, b.iiif_assoc_type_id, b.iiif_id_to AS member_id FROM iiif_assoc a JOIN iiif_assoc b ON a.iiif_id_to = b.iiif_id_from AND a.iiif_assoc_type_id = 'sc:Range' WHERE a.iiif_id_from = $1 ORDER BY b.sequence_num", [manifestId])
    const rangesToMembers = {}
    manifestRangeMembersResult.rows.forEach(memberRow => {
      const {range_id: rangeId, iiif_assoc_type_id: typeId, member_id: memberId} = memberRow
      const rangeMembers = rangesToMembers[rangeId] || (rangesToMembers[rangeId] = {ranges: [], canvases: []})
      switch (typeId) {
        case 'sc:Range':
          rangeMembers.ranges.push(memberId)
          break
        case 'sc:Canvas':
          rangeMembers.canvases.push(memberId)
          break
      }
    })
    const structures = manifestRangesResult.rows.map(rangeRow => {
      const {range_id: rangeId, label, viewing_hint: viewingHint} = rangeRow
      const members = rangesToMembers[rangeId] || {}
      return {id: rangeId, label, viewingHint, ranges: members.ranges, canvases: members.canvases}
    })
    const {iiif_id, viewing_hint: viewingHint, ...manifest} = manifestResult.rows[0]
    return {id: manifestId, viewingHint, ...manifest, structures}
  })
})

app.get('/manifest/:manifestId/canvas', (req, res) => {
  dbPoolWorker(res, async client => {
    const {manifestId} = req.params
    const rawIds = req.query.id
    if (!!!rawIds) {
      return
    }
    const ids = (rawIds instanceof Array ? rawIds : [rawIds])
    //console.log('ids', ids)
    const query = `
SELECT can_base.label, can.*
FROM
    iiif_assoc man_can_assoc JOIN iiif can_base ON
      man_can_assoc.iiif_id_to = can_base.iiif_id
      AND
      man_can_assoc.iiif_assoc_type_id = 'sc:Canvas'
    JOIN iiif_canvas can ON
      can_base.iiif_id = can.iiif_id
WHERE
    man_can_assoc.iiif_id_from = $1
    AND
    man_can_assoc.iiif_id_to IN (${ids.map((id, index) => '$' + (index + 2)).join(',')})
`.replace(/[\r\n ]+/g, ' ')
    //console.log('query', query)
    const manifestRangeMembersResult = await client.query(query, [manifestId, ...ids])
    return manifestRangeMembersResult.rows.map(({iiif_id: id, ...row}) => ({id, ...row}))
  })
})

app.listen(8080)
