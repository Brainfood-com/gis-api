#!/usr/bin/env node

const express = require('express')
const cors = require('cors')
const bodyParser = require('body-parser')
const { Client, Pool } = require('pg')

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

const app = express()
app.use(cors())

app.get('/collection', async (req, res) => {
  dbPoolWorker(res, async client => {
    const type = 'sc:Collection'
    const collectionResult = await client.query("SELECT iiif_id, label FROM iiif WHERE iiif_type_id = $1", [type])
    return collectionResult.rows.map(row => {
      return {id: row.iiif_id, label: row.label, type}
    })
  })
})

app.get('/collection/:collectionId', (req, res) => {
  dbPoolWorker(res, async client => {
    const type = 'sc:Collection'
    const {collectionId} = req.params
    const collectionResult = await client.query("SELECT iiif_id, label FROM iiif WHERE iiif_type_id = $1 AND iiif_id = $2", [type, collectionId])
    const firstRow = collectionResult.rows[0]
    const assocResult = await client.query("SELECT a.iiif_id_to, a.iiif_assoc_type_id, b.label, b.iiif_type_id FROM iiif_assoc a JOIN iiif b ON a.iiif_id_to = b.iiif_id WHERE a.iiif_id_from = $1 ORDER BY a.sequence_num, b.label", [collectionId])
    return {
      id: firstRow.iiif_id,
      label: firstRow.label,
      members: assocResult.rows.map(row => {
        return {id: row.iiif_id_to, type: row.iiif_assoc_type_id, label: row.label, type: row.iiif_type_id}
      }),
      type,
    }
  })
})

app.get('/manifest', (req, res) => {
	res.status(500).send('error')
})


app.get('/manifest/:manifestId', (req, res) => {
  dbPoolWorker(res, async client => {
    const {manifestId} = req.params
    const manifestResult = await client.query("SELECT * FROM manifest WHERE iiif_id = $1", [manifestId])
    const firstRow = manifestResult.rows[0]
    return {
      id: firstRow.iiif_id,
      attribution: firstRow.attribution,
      description: firstRow.description,
      label: firstRow.label,
      license: firstRow.license,
      logo: firstRow.logo,
      type: firstRow.iiif_type_id,
      viewingHint: firstRow.viewingHint,
    }
  })
})

app.get('/manifest/:manifestId/structures', (req, res) => {
  dbPoolWorker(res, async client => {
    const {manifestId} = req.params
    const manifestRangesResult = await client.query("SELECT a.iiif_id_to AS range_id, b.label, c.viewing_hint FROM iiif_assoc a JOIN iiif b ON a.iiif_id_to = b.iiif_id AND a.iiif_assoc_type_id = 'sc:Range' JOIN iiif_range c ON b.iiif_id = c.iiif_id WHERE a.iiif_id_from = $1 ORDER BY a.sequence_num, b.label", [manifestId])
    const manifestRangeMembersResult = await client.query(`
WITH has_point_override AS (
  SELECT
    a.external_id,
    a.iiif_override_id
  FROM
    iiif_overrides a JOIN iiif_canvas_point_overrides b ON
      a.iiif_override_id = b.iiif_override_id
  WHERE
    b.point IS NOT NULL
)
SELECT
  a.iiif_id_to AS range_id,
  b.iiif_assoc_type_id,
  b.iiif_id_to AS member_id,
  c.iiif_type_id AS member_type_id,
  EXISTS(SELECT * FROM has_point_override WHERE has_point_override.external_id = c.external_id) AS has_override_point
FROM
  iiif_assoc a JOIN iiif_assoc b ON
    a.iiif_id_to = b.iiif_id_from
    AND
    a.iiif_assoc_type_id = 'sc:Range'
  JOIN iiif c ON
    b.iiif_id_to = c.iiif_id
WHERE
  a.iiif_id_from = $1
ORDER BY
  b.sequence_num
`, [manifestId])
    const rangesToMembers = {}
    manifestRangeMembersResult.rows.forEach(memberRow => {
      const {range_id: rangeId, iiif_assoc_type_id: typeId, member_id: memberId, has_override_point: hasOverridePoint} = memberRow
      const rangeMembers = rangesToMembers[rangeId] || (rangesToMembers[rangeId] = {ranges: [], canvases: [], pointOverrideCount: 0})
      if (hasOverridePoint) {
        ++rangeMembers.pointOverrideCount
      }
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
      const {range_id: rangeId, label, iiif_type_id: type, viewing_hint: viewingHint} = rangeRow
      const members = rangesToMembers[rangeId] || {}
      return {...members, id: rangeId, label, type, viewingHint}
    })
    return structures
  })
})

async function updateTags(client, iiifId, tags) {
}

app.post('/range/:rangeId', jsonParser, (req, res) => {
  dbPoolWorker(res, async client => {
    const {rangeId} = req.params
    console.log(req.body)
    const {body: {notes, fovAngle, fovDepth, fovOrientation, tags}} = req
    const query = `
WITH range_external_id AS (
  SELECT
    external_id
  FROM
    range
  WHERE
    iiif_id = $1
), override_id AS (
  INSERT INTO iiif_overrides
    (external_id, notes)
    SELECT
      range_external_id.external_id, $2
    FROM range_external_id

  ON CONFLICT (external_id) DO UPDATE SET notes = $2
  RETURNING iiif_override_id
)
INSERT INTO iiif_range_overrides
  (iiif_override_id, fov_angle, fov_depth, fov_orientation)
  SELECT
    override_id.iiif_override_id, $3, $4, $5
  FROM override_id

  ON CONFLICT (iiif_override_id) DO UPDATE SET (fov_angle, fov_depth, fov_orientation) = ROW($3, $4, $5)
`
    const insertUpdateResult = await client.query(query, [rangeId, notes, fovAngle, fovDepth, fovOrientation])
    await updateTags(client, rangeId, tags)
    return {ok: true}
  })
})

app.post('/canvas/:canvasId', jsonParser, (req, res) => {
  dbPoolWorker(res, async client => {
    const {canvasId} = req.params
    console.log(req.body)
    const {body: {notes, tags = [], exclude = false, hole = false}} = req
    const query = `
WITH canvas_external_id AS (
  SELECT
    external_id
  FROM
    canvas
  WHERE
    iiif_id = $1
), override_id AS (
  INSERT INTO iiif_overrides
    (external_id, notes)
    SELECT
      canvas_external_id.external_id, $2
    FROM canvas_external_id

  ON CONFLICT (external_id) DO UPDATE SET notes = $2
  RETURNING iiif_override_id
)
INSERT INTO iiif_canvas_overrides
  (iiif_override_id, exclude, hole)
  SELECT
    override_id.iiif_override_id, $3, $4,
  FROM override_id

  ON CONFLICT (iiif_override_id) DO UPDATE SET (exclude, hole) = ROW($3, $4)
`
    const insertUpdateResult = await client.query(query, [canvasId, notes, exclude, hole])
    await updateTags(client, canvasId, tags)
    return {ok: true}
  })
})

app.post('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  dbPoolWorker(res, async client => {
    const {canvasId, sourceId} = req.params
    console.log(req.body)
    const {body: {priority, point}} = req
    const query = `
WITH canvas_external_id AS (
  SELECT
    external_id
  FROM
    canvas
  WHERE
    iiif_id = $1
), override_id AS (
  INSERT INTO iiif_overrides
    (external_id)
    SELECT
      canvas_external_id.external_id
    FROM canvas_external_id

    ON CONFLICT (external_id) DO UPDATE SET external_id = EXCLUDED.external_id
    RETURNING iiif_override_id
)
INSERT INTO iiif_canvas_point_overrides
  (iiif_override_id, iiif_canvas_override_source_id, priority, point)
  SELECT
    override_id.iiif_override_id, $2, $3, ST_SetSRID(ST_GeomFromGeoJSON($4), 4326)
  FROM override_id

  ON CONFLICT (iiif_override_id, iiif_canvas_override_source_id)
  DO UPDATE SET (priority, point) = ROW($3, CASE WHEN $4 IS NOT NULL THEN ST_SetSRID(ST_GeomFromGeoJSON($4), 4326) ELSE NULL END)
`
    const insertUpdateResult = await client.query(query, [canvasId, sourceId, priority, point])
    return {ok: true}
  })
})

app.get('/range/:rangeId', (req, res) => {
  dbPoolWorker(res, async client => {
    const {rangeId} = req.params
    const rangeResult = await client.query("SELECT * FROM range WHERE iiif_id = $1", [rangeId])
    const rangeOverrideResult = await client.query("SELECT * FROM range_overrides WHERE iiif_id = $1", [rangeId])
    console.log('rangeOverrideResult', rangeOverrideResult)
    const firstRow = rangeResult.rows[0]
    const firstOverrideRow = rangeOverrideResult.rows[0] || {}
    console.log('firstOverrideRow', firstOverrideRow)
    return {
      id: firstRow.iiif_id,
      label: firstRow.label,
      type: firstRow.iiif_type_id,
      viewingHint: firstRow.viewingHint,
      notes: firstOverrideRow.notes,
      fovAngle: firstOverrideRow.fov_angle,
      fovDepth: firstOverrideRow.fov_depth,
      fovOrientation: firstOverrideRow.fov_orientation,
    }
  })
})

app.get('/range/:rangeId/canvasPoints', (req, res) => {
  dbPoolWorker(res, async client => {
    const {rangeId} = req.params
    const query = `
WITH road AS (
	SELECT st_linemerge(st_collect(geom)) AS geom FROM sunset_road_merged
),
canvas_percent_placement AS (
	SELECT
		iiif.iiif_id,
		ST_LineLocatePoint((SELECT geom FROM road), ST_Centroid(ST_Collect(canvas_point_overrides.point))) AS percentage
	FROM
		iiif JOIN canvas_point_overrides ON
			iiif.external_id = canvas_point_overrides.external_id
	GROUP BY
		iiif.iiif_id
),
canvas_range_grouping AS (
	SELECT
		range_canvas.range_id,
		range_canvas.iiif_id,
		range_canvas.sequence_num,
		canvas_percent_placement.percentage,
		count(canvas_percent_placement.percentage) OVER (ORDER BY range_canvas.sequence_num) AS forward,
		count(canvas_percent_placement.percentage) OVER (ORDER BY range_canvas.sequence_num DESC) AS reverse
	FROM
		range_canvas LEFT JOIN canvas_percent_placement ON
			range_canvas.iiif_id = canvas_percent_placement.iiif_id
	WHERE
		range_canvas.range_id = $1
 	GROUP BY
 		range_canvas.range_id,
 		range_canvas.iiif_id,
 		range_canvas.sequence_num,
 		canvas_percent_placement.percentage
),
canvas_in_range_list AS (
	SELECT
		canvas_range_grouping.range_id,
		canvas_range_grouping.iiif_id,
		canvas_range_grouping.percentage,
		percent_rank() OVER (PARTITION BY canvas_range_grouping.reverse ORDER BY canvas_range_grouping.sequence_num) start_rank,
		cume_dist() OVER (PARTITION BY canvas_range_grouping.reverse ORDER BY canvas_range_grouping.sequence_num) other_rank,
		COALESCE(first_value(canvas_range_grouping.percentage) OVER (PARTITION BY canvas_range_grouping.reverse ORDER BY canvas_range_grouping.sequence_num DESC), 1) AS end,
		COALESCE(first_value(canvas_range_grouping.percentage) OVER (PARTITION BY canvas_range_grouping.forward ORDER BY canvas_range_grouping.sequence_num), 0) AS start
	FROM
		canvas_range_grouping
)
SELECT
	canvas_in_range_list.start,
	canvas_in_range_list.end,
	ST_AsGeoJSON(ST_LineInterpolatePoint((SELECT road.geom FROM road), (
			canvas_in_range_list.end -
			canvas_in_range_list.start
		) *
		CASE
			WHEN canvas_in_range_list.start IS NULL THEN
				canvas_in_range_list.start_rank
			ELSE
				canvas_in_range_list.other_rank
		END + canvas_in_range_list.start
	)) AS point,
	(SELECT json_agg(json_build_object( 'iiif_canvas_override_source_id', iiif_canvas_override_source_id, 'priority', priority, 'point', ST_AsGeoJSON(point))) FROM canvas_point_overrides WHERE external_id = range_canvas.external_id) AS overrides,
	range_canvas.*
FROM
	range_canvas JOIN canvas_in_range_list ON
    range_canvas.range_id = canvas_in_range_list.range_id
    AND
		range_canvas.iiif_id = canvas_in_range_list.iiif_id
ORDER BY
	range_canvas.iiif_id
`.replace(/[\r\n ]+/g, ' ')
    //console.log('query', query)
    const manifestRangeMembersResult = await client.query(query, [rangeId])
    return manifestRangeMembersResult.rows.map(({iiif_id: id, point, overrides, ...row}) => {
      if (overrides) {
        //console.log('about to parse', overrides.point)
        overrides.point = JSON.parse(overrides.point || null)
      }
      return ({id, point: JSON.parse(point), overrides, ...row})
    })
  })
})

app.listen(8080)
