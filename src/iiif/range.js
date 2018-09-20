import turfAlong from '@turf/along'
import turfBearing from '@turf/bearing'
import turfLength from '@turf/length'

import {getTags, updateTags} from './tags'

import {dbPoolWorker} from '../../server'

export async function getOne(client, rangeId) {
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
    fovOrientation: firstOverrideRow.fov_orientation || 'left',
    tags,
  }
}

export async function updateOne(client, rangeId, {notes, fovAngle, fovDepth, fovOrientation, tags}) {
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
}
export const setOverrides = updateOne

export async function getOverrides(client, iiifOverrideId) {
  const rangeInfo = await client.query('SELECT fov_angle, fov_depth, fov_orientation FROM iiif_range_overrides WHERE iiif_override_id = $1', [iiifOverrideId])
  const {fov_angle, fov_depth, fov_orientation} = rangeInfo.rows[0] || {}
  console.log('range:getOverrides')
  return {
    fov_angle,
    fov_depth,
    fov_orientation,
  }
}

export async function calculateRoutes(client, ...rangeIds) {
  let query = `
SELECT DISTINCT
  ST_SRID(start_point) AS start_srid,
  ST_AsGeoJSON(start_point) AS start_point,
  ST_SRID(end_point) AS end_srid,
  ST_AsGeoJSON(end_point) AS end_point
FROM
  routing_canvas_range_grouping
WHERE
  start_point IS NOT NULL
  AND
  end_point IS NOT NULL
  AND
  start_point::text != end_point::text
`.replace(/[\t\r\n ]+/g, ' ')
  if (rangeIds.length) {
    query += `AND range_id IN (${rangeIds.map((id, index) => `$${index + 1}`).join(', ')})`
  }
  const routePointsResult = await client.query(query, rangeIds)
  const pendingRouteResults = routePointsResult.rows.map(async ({start_srid: startSrid, start_point: startPoint, end_srid: endSrid, end_point: endPoint}) => {
      await client.query('SELECT plan_route(ST_SetSRID(ST_GeomFromGeoJSON($2), $1), ST_SetSRID(ST_GeomFromGeoJSON($4), $3))', [startSrid, startPoint, endSrid, endPoint])
      return true
  })
  await Promise.all(pendingRouteResults)
  return pendingRouteResults.length
}

function radiansToDegrees(radians) {
  return radians * 180 / Math.PI
}

export async function getCanvasPoints(client, rangeId) {
  await calculateRoutes(client, ...rangeId)
  const query = `
SELECT
  canvas_overrides.notes,
  canvas_overrides.exclude,
  canvas_overrides.hole,
	range_canvas.*,
  ST_AsGeoJSON(routing_canvas_range_camera.point) AS point,
  ST_AsGeoJSON(routing_canvas_range_camera.camera) AS camera,
  (SELECT array_agg(ogc_fid) FROM lariac_buildings WHERE ST_Intersects(camera, wkb_geometry)) AS buildings,
  routing_canvas_range_camera.bearing,
	(SELECT json_agg(json_build_object( 'iiif_canvas_override_source_id', iiif_canvas_override_source_id, 'priority', priority, 'point', ST_AsGeoJSON(point))) FROM canvas_point_overrides WHERE iiif_id = range_canvas.iiif_id) AS overrides
FROM
  range_canvas JOIN routing_canvas_range_camera ON
    range_canvas.range_id = routing_canvas_range_camera.range_id
    AND
    range_canvas.iiif_id = routing_canvas_range_camera.iiif_id
  LEFT JOIN canvas_overrides ON
    range_canvas.iiif_id = canvas_overrides.iiif_id
WHERE
	range_canvas.range_id = $1
ORDER BY
	iiif_id
`.replace(/[\t\r\n ]+/g, ' ')
  const manifestRangeMembersResult = await client.query(query, [rangeId])
  const canvasPoints = manifestRangeMembersResult.rows.map(({iiif_id: id, point, camera, buildings, overrides, bearing, ...row}, index) => {
    if (overrides) {
      overrides.forEach(override => {
        override.point = JSON.parse(override.point || null)
      })
    }

    const result = {
      id,
      point: point ? JSON.parse(point) : undefined,
      camera: camera ? JSON.parse(camera) : undefined,
      buildings,
      overrides,
      bearing: radiansToDegrees(bearing),
      ...row
    }
    return result
  })
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

export async function getGeoJSON(client, rangeId) {
  const range = await exports.getOne(client, rangeId)
  const {fovOrientation} = range


  const canvasPoints = await getCanvasPoints(client, rangeId)
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
