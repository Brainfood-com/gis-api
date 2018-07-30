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

async function updateTags(client, iiifId, tags) {
  const query = `
WITH iiif_object AS (
  SELECT iiif_type_id, external_id FROM iiif WHERE iiif_id = $1
), tag_list AS (
  SELECT ROW_NUMBER() OVER() AS sequence_num, tag.tag FROM (SELECT UNNEST($2::text[]) AS tag) tag
), tag_id AS (
  INSERT INTO iiif_tags (iiif_type_id, tag) SELECT iiif_object.iiif_type_id, tag_list.tag FROM iiif_object CROSS JOIN tag_list
  ON CONFLICT (iiif_type_id, tag) DO UPDATE SET tag = EXCLUDED.tag
  RETURNING *
), override_id AS (
  INSERT INTO iiif_overrides (external_id) SELECT iiif_object.external_id FROM iiif_object
  ON CONFLICT (external_id) DO UPDATE SET external_id = EXCLUDED.external_id
  RETURNING iiif_override_id
), delete_ignore AS (
  DELETE FROM iiif_overrides_tags USING tag_id WHERE iiif_overrides_tags.iiif_tag_id NOT IN (tag_id.iiif_tag_id)
)
INSERT INTO iiif_overrides_tags (iiif_override_id, iiif_tag_id, sequence_num)
SELECT
  c.iiif_override_id, b.iiif_tag_id, a.sequence_num
FROM
  tag_list a JOIN tag_id b ON
    a.tag = b.tag
  CROSS JOIN override_id c
ON CONFLICT (iiif_override_id, iiif_tag_id) DO UPDATE SET sequence_num = EXCLUDED.sequence_num
`
  return await client.query(query, [iiifId, tags])
}

async function getTags(client, iiifId) {
  const query = `
SELECT
  b.tag
FROM
  iiif a JOIN iiif_tags b ON
    a.iiif_type_id = b.iiif_type_id
  JOIN iiif_overrides c ON
    a.external_id = c.external_id
  JOIN iiif_overrides_tags d ON
    c.iiif_override_id = d.iiif_override_id
    AND
    b.iiif_tag_id = d.iiif_tag_id
WHERE
  iiif_id = $1
ORDER BY
  d.sequence_num
`
  const tagsResult = await client.query(query, [iiifId])
  return tagsResult.rows.map(row => row.tag)
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
    const tags = await getTags(client, collectionId)
    return {
      id: firstRow.iiif_id,
      label: firstRow.label,
      manifests: assocResult.rows.map(row => {
        return {id: row.iiif_id_to, type: row.iiif_assoc_type_id, label: row.label, type: row.iiif_type_id}
      }),
      type,
      tags,
    }
  })
})

app.post('/collection/:collectionId', jsonParser, (req, res) => {
  dbPoolWorker(res, async client => {
    const {collectionId} = req.params
    const {body: {notes, tags}} = req
    const query = `
WITH collection_external_id AS (
  SELECT
    external_id
  FROM
    collection
  WHERE
    iiif_id = $1
)
INSERT INTO iiif_overrides
  (external_id, notes)
  SELECT
    collection_external_id.external_id, $2
  FROM collection_external_id

  ON CONFLICT (external_id) DO UPDATE SET notes = $2
`
    const insertUpdateResult = await client.query(query, [collectionId, notes])
    await updateTags(client, collectionId, tags)
    return {ok: true}
  })
})

app.get('/manifest', (req, res) => {
	res.status(500).send('error')
})


app.get('/manifest/:manifestId', (req, res) => {
  dbPoolWorker(res, async client => {
    const {manifestId} = req.params
    const manifestResult = await client.query("SELECT * FROM manifest WHERE iiif_id = $1", [manifestId])
    const tags = await getTags(client, manifestId)
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
      tags,
    }
  })
})

app.post('/manifest/:manifestId', jsonParser, (req, res) => {
  dbPoolWorker(res, async client => {
    const {manifestId} = req.params
    const {body: {notes, tags}} = req
    const query = `
WITH manifest_external_id AS (
  SELECT
    external_id
  FROM
    manifest
  WHERE
    iiif_id = $1
)
INSERT INTO iiif_overrides
  (external_id, notes)
  SELECT
    manifest_external_id.external_id, $2
  FROM manifest_external_id

  ON CONFLICT (external_id) DO UPDATE SET notes = $2
`
    const insertUpdateResult = await client.query(query, [manifestId, notes])
    await updateTags(client, manifestId, tags)
    return {ok: true}
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

app.get('/range/:rangeId', (req, res) => {
  dbPoolWorker(res, async client => {
    const {rangeId} = req.params
    const rangeResult = await client.query("SELECT * FROM range WHERE iiif_id = $1", [rangeId])
    const rangeOverrideResult = await client.query("SELECT * FROM range_overrides WHERE iiif_id = $1", [rangeId])
    const tags = await getTags(client, rangeId)
    const firstRow = rangeResult.rows[0]
    const firstOverrideRow = rangeOverrideResult.rows[0] || {}
    return {
      id: firstRow.iiif_id,
      label: firstRow.label,
      type: firstRow.iiif_type_id,
      viewingHint: firstRow.viewingHint,
      notes: firstOverrideRow.notes,
      fovAngle: firstOverrideRow.fov_angle,
      fovDepth: firstOverrideRow.fov_depth,
      fovOrientation: firstOverrideRow.fov_orientation,
      tags,
    }
  })
})

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

app.get('/range/:rangeId/canvasPoints', (req, res) => {
  dbPoolWorker(res, async client => {
    const {rangeId} = req.params
    const query = `
WITH road AS (
	SELECT st_linemerge(st_collect(geom)) AS geom FROM sunset_road_merged
),
road_meta AS (
  SELECT
    ST_StartPoint(road.geom) AS start_point,
    gisapp_nearest_edge(ST_StartPoint(road.geom)) AS start_edge,
    ST_EndPoint(road.geom) AS end_point,
    gisapp_nearest_edge(ST_EndPoint(road.geom)) AS end_edge
  FROM
    road
),
canvas_point_override AS (
	SELECT
		iiif.iiif_id,
		canvas_point_overrides.point,
    gisapp_nearest_edge(canvas_point_overrides.point) AS edge
	FROM
		iiif JOIN canvas_point_overrides ON
			iiif.external_id = canvas_point_overrides.external_id

	GROUP BY
		iiif.iiif_id,
    canvas_point_overrides.point
),
canvas_range_grouping AS (
	SELECT
		range_canvas.range_id,
		range_canvas.iiif_id,
		range_canvas.sequence_num,
		canvas_point_override.point,
		canvas_point_override.edge,
		canvas_overrides.exclude,
		count(canvas_point_override.point) OVER (PARTITION BY canvas_overrides.exclude IS NULL OR canvas_overrides.exclude = false ORDER BY range_canvas.sequence_num) AS forward,
		count(canvas_point_override.point) OVER (PARTITION BY canvas_overrides.exclude IS NULL OR canvas_overrides.exclude = false ORDER BY range_canvas.sequence_num DESC) AS reverse
	FROM
		range_canvas LEFT JOIN canvas_point_override ON
			range_canvas.iiif_id = canvas_point_override.iiif_id
		LEFT JOIN canvas_overrides ON
			range_canvas.iiif_id = canvas_overrides.iiif_id
	WHERE
		range_canvas.range_id = $1
 	GROUP BY
 		range_canvas.range_id,
 		range_canvas.iiif_id,
		canvas_overrides.exclude,
 		range_canvas.sequence_num,
		canvas_point_override.point,
		canvas_point_override.edge
),
canvas_in_range_list AS (
	SELECT
		canvas_range_grouping.*,
		percent_rank() OVER (PARTITION BY canvas_range_grouping.reverse ORDER BY canvas_range_grouping.sequence_num) start_rank,
		cume_dist() OVER (PARTITION BY canvas_range_grouping.reverse ORDER BY canvas_range_grouping.sequence_num) other_rank,
		COALESCE(first_value(canvas_range_grouping.point) OVER (PARTITION BY canvas_range_grouping.reverse ORDER BY canvas_range_grouping.sequence_num DESC), (SELECT end_point FROM road_meta)) AS end_point,
		COALESCE(first_value(canvas_range_grouping.point) OVER (PARTITION BY canvas_range_grouping.forward ORDER BY canvas_range_grouping.sequence_num), (SELECT start_point FROM road_meta)) AS start_point
	FROM
		canvas_range_grouping
)
SELECT
	ST_AsGeoJSON(CASE
    WHEN canvas_overrides.exclude = true THEN NULL
    WHEN canvas_in_range_list.point IS NOT NULL THEN canvas_in_range_list.point
    ELSE ST_LineInterpolatePoint(plan_route(canvas_in_range_list.start_point, canvas_in_range_list.end_point), canvas_in_range_list.other_rank)
	END) AS point,

	(SELECT json_agg(json_build_object( 'iiif_canvas_override_source_id', iiif_canvas_override_source_id, 'priority', priority, 'point', ST_AsGeoJSON(point))) FROM canvas_point_overrides WHERE external_id = range_canvas.external_id) AS overrides,
  canvas_overrides.notes,
  canvas_overrides.exclude,
  canvas_overrides.hole,
	range_canvas.*
FROM
	range_canvas JOIN canvas_in_range_list ON
    range_canvas.range_id = canvas_in_range_list.range_id
    AND
		range_canvas.iiif_id = canvas_in_range_list.iiif_id
  LEFT JOIN canvas_overrides ON
    range_canvas.iiif_id = canvas_overrides.iiif_id
ORDER BY
	range_canvas.iiif_id
`.replace(/[\t\r\n ]+/g, ' ')
    console.log('query', query)
    const manifestRangeMembersResult = await client.query(query, [rangeId])
    return manifestRangeMembersResult.rows.map(({iiif_id: id, point, overrides, ...row}) => {
      if (overrides) {
        overrides.forEach(override => {
          override.point = JSON.parse(override.point || null)
        })
        ////console.log('about to parse', overrides.point)
        //overrides.point = JSON.parse(overrides.point || null)
      }
      return ({id, point: JSON.parse(point), overrides, ...row})
    })
  })
})

app.get('/canvas/:canvasId', (req, res) => {
  dbPoolWorker(res, async client => {
    const {canvasId} = req.params
    const canvasResult = await client.query("SELECT * FROM canvas WHERE iiif_id = $1", [canvasId])
    const canvasOverrideResult = await client.query("SELECT * FROM canvas_overrides WHERE iiif_id = $1", [canvasId])
    const tags = await getTags(client, canvasId)
    const firstRow = canvasResult.rows[0]
    const firstOverrideRow = canvasOverrideResult.rows[0] || {}
    return {
      id: firstRow.iiif_id,
      label: firstRow.label,
      type: firstRow.iiif_type_id,
      format: firstRow.format,
      height: firstRow.height,
      image: firstRow.image,
      thumbnail: firstRow.thumbnail,
      width: firstRow.width,

      notes: firstOverrideRow.notes,
      exclude: firstOverrideRow.exclude,
      hole: firstOverrideRow.hole,
      tags,
      /*
      fovAngle: firstOverrideRow.fov_angle,
      fovDepth: firstOverrideRow.fov_depth,
      fovOrientation: firstOverrideRow.fov_orientation,
      */
    }
  })
})

app.post('/canvas/:canvasId', jsonParser, (req, res) => {
  dbPoolWorker(res, async client => {
    const {canvasId} = req.params
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
    override_id.iiif_override_id, $3, $4
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
    const pointAdjustQuery = `

WITH parsed AS (
  SELECT ST_SetSRID(ST_GeomFromGeoJSON($1), 4326) AS point
),
nearest_edge AS (
  SELECT
    gisapp_nearest_edge(parsed.point) AS edge
  FROM
    parsed
)
SELECT
  ST_AsGeoJSON(ST_ClosestPoint(tl_2017_06037_edges.wkb_geometry, parsed.point)) AS point
FROM
  tl_2017_06037_edges JOIN nearest_edge ON tl_2017_06037_edges.ogc_fid = nearest_edge.edge,
  parsed
`
    const adjustResult = await client.query(pointAdjustQuery, [point])
    const adjustedPoint = adjustResult.rowCount ? adjustResult.rows[0].point : point
    console.log('adjustResult', adjustResult)
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
    const insertUpdateResult = await client.query(query, [canvasId, sourceId, priority, adjustedPoint])
    return {ok: true}
  })
})

app.delete('/canvas/:canvasId/point/:sourceId', jsonParser, (req, res) => {
  dbPoolWorker(res, async client => {
    const {canvasId, sourceId} = req.params
    const query = `
WITH override_id AS (
  SELECT
    iiif_override_id
  FROM
    canvas_point_overrides
  WHERE
    iiif_id = $1
)
DELETE FROM iiif_canvas_point_overrides
  USING override_id
  WHERE
    iiif_canvas_point_overrides.iiif_override_id = override_id.iiif_override_id
    AND
    iiif_canvas_override_source_id = $2
`
    const deleteResult = await client.query(query, [canvasId, sourceId])
    return {ok: true}
  })
})

app.listen(8080)
