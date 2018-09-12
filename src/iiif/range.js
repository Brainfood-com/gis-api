const turfAlong = require('@turf/along')
const turfBearing = require('@turf/bearing')
const turfLength = require('@turf/length')

const iiifTags = require('./tags')

exports.getOne = async function getOne(client, rangeId) {
  const rangeResult = await client.query("SELECT * FROM range WHERE iiif_id = $1", [rangeId])
  const rangeOverrideResult = await client.query("SELECT * FROM range_overrides WHERE iiif_id = $1", [rangeId])
  const tags = await iiifTags.getOne(client, rangeId)
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
    fovOrientation: firstOverrideRow.fov_orientation || 'left',
    tags,
  }
}

exports.updateOne = exports.setOverrides = async function updateOne(client, rangeId, {notes, fovAngle, fovDepth, fovOrientation, tags}) {
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
  await iiifTags.updateOne(client, rangeId, tags)
  return {ok: true}
}

exports.getOverrides = async function getOverrides(client, iiifOverrideId) {
  const rangeInfo = await client.query('SELECT fov_angle, fov_depth, fov_orientation FROM iiif_range_overrides WHERE iiif_override_id = $1', [iiifOverrideId])
  const {fov_angle, fov_depth, fov_orientation} = rangeInfo.rows[0] || {}
  console.log('range:getOverrides')
  return {
    fov_angle,
    fov_depth,
    fov_orientation,
  }
}

exports.getCanvasPoints = async function getCanvasPoints(client, rangeId) {
  const query = `
WITH
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
		case when canvas_overrides.exclude is true then -1 else count(canvas_point_override.point) OVER (PARTITION BY canvas_overrides.exclude IS NULL OR canvas_overrides.exclude = false ORDER BY range_canvas.sequence_num) end AS forward,
		case when canvas_overrides.exclude is true then -1 else count(canvas_point_override.point) OVER (PARTITION BY canvas_overrides.exclude IS NULL OR canvas_overrides.exclude = false ORDER BY range_canvas.sequence_num DESC) end AS reverse
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
		first_value(canvas_range_grouping.point) OVER (PARTITION BY canvas_range_grouping.reverse ORDER BY canvas_range_grouping.sequence_num DESC) AS end_point,
		first_value(canvas_range_grouping.point) OVER (PARTITION BY canvas_range_grouping.forward ORDER BY canvas_range_grouping.sequence_num) AS start_point
	FROM
		canvas_range_grouping
)
SELECT
  canvas_overrides.notes,
  canvas_overrides.exclude,
  canvas_overrides.hole,
	range_canvas.*,
  canvas_in_range_list.other_rank,
  ST_AsGeoJSON(canvas_in_range_list.point) AS point,
  ST_AsGeoJSON(canvas_in_range_list.start_point) AS start_point,
  ST_AsGeoJSON(canvas_in_range_list.end_point) AS end_point,
	(SELECT json_agg(json_build_object( 'iiif_canvas_override_source_id', iiif_canvas_override_source_id, 'priority', priority, 'point', ST_AsGeoJSON(point))) FROM canvas_point_overrides WHERE external_id = range_canvas.external_id) AS overrides
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
  const manifestRangeMembersResult = await client.query(query, [rangeId])
  const routeLookup = {}
  manifestRangeMembersResult.rows.forEach(({start_point: startPoint, end_point: endPoint}) => {
    if (!startPoint || !endPoint || endPoint === startPoint) {
      return
    }
    const startPointNode = routeLookup[startPoint] || (routeLookup[startPoint] = {})
    if (!startPointNode[endPoint]) {
      startPointNode[endPoint] = client.query('SELECT ST_AsGeoJSON(plan_route(ST_SetSRID(ST_GeomFromGeoJSON($1), 4326), ST_SetSRID(ST_GeomFromGeoJSON($2), 4326))) AS route', [startPoint, endPoint])
    }
  })
  const pendingPromises = []
  Object.keys(routeLookup).forEach(startPoint => {
    const startPointNode = routeLookup[startPoint]
    Object.keys(startPointNode).map(async endPoint => {
      const firstRow = (await (startPointNode[endPoint])).rows[0]
      const route = JSON.parse(firstRow.route)
      startPointNode[endPoint] = {
        route,
        length: turfLength(route),
      }
    }).forEach(promise => pendingPromises.push(promise))
  })
  await Promise.all(pendingPromises)
  const validPoints = []
  const bearingPoints = new Array(2)
  let previousValidPointResult
  const canvasPoints = manifestRangeMembersResult.rows.map(({iiif_id: id, point, start_point: startPoint, end_point: endPoint, overrides, other_rank: otherRank, ...row}, index) => {
    if (overrides) {
      overrides.forEach(override => {
        override.point = JSON.parse(override.point || null)
      })
    }

    if (point) {
      point = JSON.parse(point)
    } else if (!row.exclude && startPoint && endPoint) {
      const routeData = routeLookup[startPoint][endPoint]
      point = turfAlong(routeData.route, otherRank * routeData.length).geometry
    }
    const result = {id, point, overrides, ...row}
    if (point) {
      bearingPoints[1] = [point.coordinates[0], point.coordinates[1]]
      if (validPoints.length) {
        validPoints[validPoints.length - 1].bearing = turfBearing(bearingPoints[0], bearingPoints[1])
      }
      bearingPoints[0] = bearingPoints[1]
      validPoints.push(result)
    }
    return result
  })
  if (validPoints.length > 1) {
    validPoints[validPoints.length - 1].bearing = validPoints[validPoints.length - 2].bearing
  }
  return canvasPoints
/*

SELECT
	ST_AsGeoJSON(CASE
    WHEN canvas_overrides.exclude = true THEN NULL
    WHEN canvas_in_range_list.point IS NOT NULL THEN canvas_in_range_list.point
    ELSE ST_LineInterpolatePoint(canvas_range_route.route, canvas_in_range_list.other_rank)
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
  LEFT JOIN canvas_range_route ON
    canvas_in_range_list.range_id = canvas_range_route.range_id
    AND
    canvas_in_range_list.start_point = canvas_range_route.start_point
    AND
    canvas_in_range_list.end_point = canvas_range_route.end_point
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
  */
}

exports.getGeoJSON = async function getGeoJSON(client, rangeId) {
  const range = await exports.getOne(client, rangeId)
  const {fovOrientation} = range


  const canvasPoints = await exports.getCanvasPoints(client, rangeId)
  return {
    type: 'FeatureCollection',
    metadata: {
      imagecount: canvasPoints.length,
      date: '?',
      starttime: '?',
      stoptime: '?',
      startintersection: '?',
      stopintersection: '?',
      photographer: '?',
      driver: '?',
      subjectstreet: '?',
      camerainfo: '?'
    },
    features: canvasPoints.map(canvasPoint => {
      const {bearing, point} = canvasPoint
      const cameraDirection = (bearing + (fovOrientation === 'left' ? 0 : 180)) % 360
      const streetview = point ? `https://maps.google.com/maps/@?api=1&map_action=pano&viewpoint=${point.coordinates[1]},${point.coordinates[0]}&heading=${cameraDirection}` : null
        return {
          type: 'Feature',
          geometry: point,
          properties: {
            filename: canvasPoint.image,
            bearing,
            distance: '?',
            date: '?',
            time: '?',
            distance: '?',
            streetview,
            streetaddress: '?',
            structureextant: '?',
            yearbuilt: '?',
            zoning: '?',
            ocr: [
              '?',
            ],
            tags: [
              '?',
            ],
            colormetadata: {
              hsv: '?',
              grey: '',
            },
            taxlots: [
              {
                ain: 'XXXXX01',
                yearbuilt: 1951,
              },
              {
                ain: 'XXXXX02',
                yearbuilt: 1951,
              },
              {
                ain: 'XXXXX03',
                yearbuilt: 1968,
              }
            ]
          },
        }
    }),
  }
}
