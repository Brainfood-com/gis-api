import turfAlong from '@turf/along'
import turfBearing from '@turf/bearing'
import turfLength from '@turf/length'
import promiseLimit from 'promise-limit'
import getenv from 'getenv'
import csvStringify from 'csv-stringify/lib/sync'

import {processGoogleVision} from './canvas'
import {getTags, updateTags} from './tags'
import * as iiifManifest from './manifest'
import * as iiifValues from './values'
import * as buildings from '../buildings'

import {dbPoolWorker} from '../dbPool'

const parallelRouteLimit = promiseLimit(getenv.int('CALCULATE_ROUTE_CONCURRENCY', 1))

export async function getParents(client, rangeId) {
  const dbResult = await client.query("SELECT DISTINCT a.iiif_id_from FROM iiif_assoc a JOIN iiif b ON a.iiif_id_from = b.iiif_id AND b.iiif_type_id = 'sc:Manifest' WHERE a.iiif_id_to = $1 AND a.iiif_assoc_type_id = 'sc:Range'", [rangeId])
  return dbResult.rows.map(row => ['sc:Manifest', row.iiif_id_from])
}

export async function getOne(client, rangeId) {
  const rangeResult = await client.query("SELECT * FROM range WHERE iiif_id = $1", [rangeId])
  const rangeOverrideResult = await client.query("SELECT * FROM range_overrides WHERE iiif_id = $1", [rangeId])
  const tags = await getTags(client, rangeId)
  const firstRow = rangeResult.rows[0]
  const firstOverrideRow = rangeOverrideResult.rows[0] || {}
  return {
    id: firstRow.iiif_id,
    externalId: firstRow.external_id,
    label: firstRow.label,
    type: firstRow.iiif_type_id,
    viewingHint: firstRow.viewingHint,
    notes: firstOverrideRow.notes,
    reverse: firstOverrideRow.reverse,
    fovAngle: firstOverrideRow.fov_angle,
    fovDepth: firstOverrideRow.fov_depth,
    fovOrientation: firstOverrideRow.fov_orientation || 'left',
    tags,
  }
}

export async function updateOne(client, rangeId, {notes, reverse, fovAngle, fovDepth, fovOrientation, tags}) {
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
  (iiif_override_id, reverse, fov_angle, fov_depth, fov_orientation)
  SELECT
    override_id.iiif_override_id, $3, $4, $5, $6
  FROM override_id

  ON CONFLICT (iiif_override_id) DO UPDATE SET (reverse, fov_angle, fov_depth, fov_orientation) = ROW($3, $4, $5, $6)
`
  const insertUpdateResult = await client.query(query, [rangeId, notes, reverse, fovAngle, fovDepth, fovOrientation])
  await updateTags(client, rangeId, tags)
  return {ok: true}
}
export const setOverrides = updateOne

export async function getOverrides(client, iiifOverrideId) {
  const rangeInfo = await client.query('SELECT reverse, fov_angle, fov_depth, fov_orientation FROM iiif_range_overrides WHERE iiif_override_id = $1', [iiifOverrideId])
  const {reverse, fov_angle, fov_depth, fov_orientation} = rangeInfo.rows[0] || {}
  return {
    reverse,
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
  const pendingRouteResults = await parallelRouteLimit.map(routePointsResult.rows, async ({start_srid: startSrid, start_point: startPoint, end_srid: endSrid, end_point: endPoint}) => {
    return await dbPoolWorker(async client => {
      await client.query('SELECT plan_route(ST_SetSRID(ST_GeomFromGeoJSON($2), $1), ST_SetSRID(ST_GeomFromGeoJSON($4), $3))', [startSrid, startPoint, endSrid, endPoint])
      return true
    })
  })
  return pendingRouteResults.length
}

function radiansToDegrees(radians) {
  return radians * 180 / Math.PI
}

export async function getCanvasPoints(client, rangeId) {
  await calculateRoutes(client, rangeId)
  const query = `
SELECT
  canvas_overrides.notes,
  canvas_overrides.exclude,
  canvas_overrides.hole,
	range_canvas.*,
  ST_AsGeoJSON(routing_canvas_range_interpolation_cache.point) AS point,
  ST_AsGeoJSON(routing_canvas_range_interpolation_cache.camera) AS camera,
  (SELECT array_agg(ogc_fid) FROM lariac_buildings WHERE ST_Intersects(camera, wkb_geometry)) AS buildings,
  routing_canvas_range_interpolation_cache.bearing,
  iiif_values.values,
	(SELECT json_agg(json_build_object( 'iiif_canvas_override_source_id', iiif_canvas_override_source_id, 'priority', priority, 'point', ST_AsGeoJSON(point))) FROM canvas_point_overrides WHERE iiif_id = range_canvas.iiif_id AND point IS NOT NULL) AS overrides
FROM
  range_canvas JOIN routing_canvas_range_interpolation_cache ON
    range_canvas.range_id = routing_canvas_range_interpolation_cache.range_id
    AND
    range_canvas.iiif_id = routing_canvas_range_interpolation_cache.iiif_id
  LEFT JOIN canvas_overrides ON
    range_canvas.iiif_id = canvas_overrides.iiif_id
  LEFT JOIN iiif_values ON
    range_canvas.iiif_id = iiif_values.iiif_id
WHERE
	range_canvas.range_id = $1
ORDER BY
	iiif_id
`.replace(/[\t\r\n ]+/g, ' ')
  const manifestRangeMembersResult = await client.query(query, [rangeId])
  const canvasPoints = manifestRangeMembersResult.rows.map(({iiif_id: id, point, camera, buildings, overrides, bearing, google_vision, values, ...row}, index) => {
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
      googleVision: processGoogleVision(google_vision),
      values: iiifValues.parseRows(values),
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

const addressFields = ['house_no', 'house_fraction', 'street_direction', 'street_name', 'unit_no', 'city', 'zip_code5']
const addressFieldSeparators = {
    unit_no: ',',
}

async function gatherAllExportData(client, rangeId, canvasId) {
  const range = await exports.getOne(client, rangeId)
  const manifest = await exports.getParents(client, rangeId).then(parents => iiifManifest.getOne(client, parents[0][1]))
  const manifestValues = await iiifValues.getValues(client, manifest.id)

  const allCanvasPoints = await getCanvasPoints(client, rangeId)
  const canvasPoints = canvasId ? allCanvasPoints.filter(canvasPoint => canvasPoint.id === canvasId) : allCanvasPoints
  const allBuildings = {}
  canvasPoints.forEach(canvasPoint => (canvasPoint.buildings || []).forEach(buildingId => allBuildings[buildingId] = false))
  const ignore = (await buildings.getBuildings(client, ...(Object.keys(allBuildings).map(id => parseInt(id))))).forEach(building => {
    allBuildings[building.id] = building
  })

  return {
    manifest,
    manifestValues,
    range,
    canvasPoints,
    allBuildings,
  }
}

export async function getGeoJSON(client, rangeId) {
  const exportData = await gatherAllExportData(client, rangeId)
  return translateToGeoJSON(exportData)
}

export async function getCanvasJSON(client, rangeId, canvasId) {
  const exportData = await gatherAllExportData(client, rangeId, canvasId)
  const {range} = exportData
  const buildings = {}, taxlots = {}
  Object.values(exportData.allBuildings).forEach(buildingAndTaxData => {
    const {taxdata, geojson, ...rest} = buildingAndTaxData
    const {id} = rest
    buildings[id] = rest
    if (taxdata) {
      const {ain} = taxdata
      taxlots[ain] = taxdata
    }
  })
  return {
    range,
    buildings: Object.values(buildings),
    taxlots: Object.values(taxlots),
    canvas: exportData.canvasPoints[0],
  }
}

export async function dataExport(client, rangeId) {
  const exportData = await gatherAllExportData(client, rangeId)
  const {manifestValues, range} = exportData
  const {fovOrientation} = range
  const allBuildings = {}, allTaxlots = {}
  Object.values(exportData.allBuildings).forEach(buildingAndTaxData => {
    const {taxdata, geojson, ...rest} = buildingAndTaxData
    const {id} = rest
    allBuildings[id] = rest
    if (taxdata) {
      const {ain} = taxdata
      allTaxlots[ain] = taxdata
    }
  })
  return {
    '.export': translateToGeoJSON(exportData),
    '-range.csv': csvStringify([
      {
        imagecount: exportData.canvasPoints.length,
        year: manifestValues.year,
        /*
        date: '?',
        starttime: '?',
        stoptime: '?',
        startintersection: '?',
        stopintersection: '?',
        photographer: '?',
        driver: '?',
        subjectstreet: '?',
        camerainfo: '?'
        */
      },
    ], {header: true}),
    '-buildings.csv': csvStringify(Object.values(allBuildings), {header: true}),
    '-taxlots.csv': csvStringify(Object.values(allTaxlots), {header: true}),
    '-canvasPoints.csv': csvStringify(exportData.canvasPoints.map(canvasPoint => {
      const {bearing, buildings, camera, googleVision, overrides, point, ...rest} = canvasPoint
      const cameraDirection = (bearing + (fovOrientation === 'left' ? 90 : -90)) % 360
      return {
        ...rest,
        latitude: point ? point.coordinates[1] : undefined,
        longitude: point ? point.coordinates[0] : undefined,
        streetview: point ? `https://maps.google.com/maps/@?api=1&map_action=pano&viewpoint=${point.coordinates[1]},${point.coordinates[0]}&heading=${cameraDirection}` : null,
        ...((buildings || []).reduce((accumulator, buildingId) => {
          if (buildingId) {
            accumulator.buildings = accumulator.buildings ? accumulator.buildings + ',' + buildingId : buildingId
            const {[buildingId]: {ain} = {}} = allBuildings
            const {[ain]: taxlot} = allTaxlots
            if (taxlot) {
              accumulator.taxlots = accumulator.taxlots ?  accumulator.taxlots + ',' + ain : ain
            }
          }
          return accumulator
        }, {buildings: '', taxlots: ''}))
      }
    }), {header: true}),
  }
}

function translateToGeoJSON(exportData) {
  const {manifestValues, range, canvasPoints, allBuildings} = exportData
  const {fovOrientation} = range

  return {
    type: 'FeatureCollection',
    metadata: {
      imagecount: canvasPoints.length,
      year: manifestValues.year,
      /*
      date: '?',
      starttime: '?',
      stoptime: '?',
      startintersection: '?',
      stopintersection: '?',
      photographer: '?',
      driver: '?',
      subjectstreet: '?',
      camerainfo: '?'
      */
    },
    features: canvasPoints.map(canvasPoint => {
      const {id, bearing, point, googleVision = {}} = canvasPoint
      const cameraDirection = (bearing + (fovOrientation === 'left' ? -90 : 90)) % 360
      const pointBuildings = (canvasPoint.buildings || []).map(id => allBuildings[id]).filter(building => building)
      const discoveredTaxData = {
        yearbuilt: null,
      }
      const taxlots = pointBuildings.map(building => {
        const {ain} = building
        const taxdata = building.taxdata || {}
        const address = addressFields.map((fieldName, index) => {
          const {[fieldName]: fieldValue = ''} = taxdata
          return [index === 0 ? '' : addressFieldSeparators[fieldName] || ' ', typeof fieldValue === 'string' ? fieldValue.trim() : fieldValue]
        }).filter(fieldValue =>
          fieldValue[1]
        ).map(fieldValue =>
          fieldValue.join('')
        ).join('')
        // house_no house_fraction street_direction street_name, unit_no
        // city zip_code5
        const {year_built} = taxdata
        if (!discoveredTaxData.yearBuilt) {
          discoveredTaxData.yearBuilt = year_built
        } else if (year_built < discoveredTaxData.year_built) {
          discoveredTaxData.yearBuilt = year_built
        }
        return {
          ain: building.ain,
          yearbuilt: year_built,
          address,
        }
      })
      const addr_parts = [canvasPoint.addr_number, canvasPoint.addr_fullname, canvasPoint.addr_zipcode]
      const streetview = point ? `https://maps.google.com/maps/@?api=1&map_action=pano&viewpoint=${point.coordinates[1]},${point.coordinates[0]}&heading=${cameraDirection}` : null
        return {
          type: 'Feature',
          geometry: point,
          properties: {
            filename: canvasPoint.image,
            bearing,
            //distance: '?',
            //date: '?',
            //time: '?',
            streetview,
            //streetaddress: addr_parts.filter(item => item).join(' '),
            //structureextant: '?',
            //yearbuilt: discoveredTaxData.yearBuilt,
            //zoning: '?',
            ocr: googleVision.ocr,
            tags: googleVision.labels,
            colormetadata: {
              hsv: googleVision.hsv,
              grey: googleVision.grey,
            },
            taxlots,
          },
        }
    }),
  }
}
