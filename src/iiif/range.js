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
    fovOrientation: firstOverrideRow.fov_orientation,
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
}

exports.getGeoJSON = async function getGeoJSON(client, rangeId) {
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
      const point = canvasPoint.point
      const streetview = point ? `https://maps.google.com/maps?q=${point.coordinates[1]},${point.coordinates[0]}&cbll=${point.coordinates[1]},${point.coordinates[0]}&layer=c` : null
        return {
          type: 'Feature',
          geometry: point,
          properties: {
            filename: canvasPoint.image,
            bearing: '?',
            distance: '?',
            date: '?',
            time: '?',
            bearing: '?',
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
