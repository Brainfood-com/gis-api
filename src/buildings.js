
import {processGoogleVision} from './iiif/canvas'

export async function getBuildings(client, ...ogcFids) {
  let query = `
SELECT
  a.ogc_fid, a.code, a.bld_id, a.height, a.elev, a.area, a.source, a.date_, a.ain, a.shape_leng, a.shape_area,
  row_to_json(b) AS taxdata,
  ST_AsGeoJSON(wkb_geometry) AS geojson
FROM
  lariac_buildings a LEFT JOIN LATERAL (
		WITH max AS (
			SELECT max(roll_year) AS roll_year FROM taxdata WHERE a.ain::numeric = taxdata.ain
		)
		SELECT
			*
		FROM
			taxdata
		WHERE
			a.ain::numeric = ain
			AND
			roll_year = (SELECT roll_year FROM max)
	) b ON
			a.ain::numeric = b.ain
WHERE
`.replace(/[\t\r\n ]+/g, ' ')
  if (!ogcFids.length) {
    return []
  }
  query += ` a.ogc_fid IN (${ogcFids.map((id, index) => `$${index + 1}`).join(', ')})`
  const result = await client.query(query, ogcFids)
  return result.rows.map(({ogc_fid: id, geojson, ...rest}) => {
    return {...rest, id, geojson: geojson ? JSON.parse(geojson) : null}
  })
}

function radiansToDegrees(radians) {
  return radians * 180 / Math.PI
}

export async function getBuildingCanvases(client, ogcFid) {
  let query = `
SELECT
  b.ogc_fid,
  c.bearing,
  ST_Distance(c.point, b.wkb_geometry) AS point_building_distance,
  ST_AsGeoJSON(c.point) AS point,
  ST_AsGeoJSON(c.camera) AS camera,
  canvas_overrides.notes,
  canvas_overrides.exclude,
  canvas_overrides.hole,
	(SELECT json_agg(json_build_object( 'iiif_canvas_override_source_id', iiif_canvas_override_source_id, 'priority', priority, 'point', ST_AsGeoJSON(point))) FROM canvas_point_overrides WHERE iiif_id = d.iiif_id) AS overrides,
  d.*
FROM
  rcri_buildings a JOIN lariac_buildings b ON
    a.building_id = b.ogc_fid
  JOIN routing_canvas_range_interpolation_cache c ON
    a.range_id = c.range_id
    AND
    a.iiif_id = c.iiif_id
  JOIN range_canvas d ON
    c.range_id = d.range_id
    AND
    c.iiif_id = d.iiif_id
  LEFT JOIN canvas_overrides ON
    d.iiif_id = canvas_overrides.iiif_id
WHERE
  b.ogc_fid = $1
ORDER BY
  d.range_id, d.sequence_num
`.replace(/[\t\r\n ]+/g, ' ')
  const result = await client.query(query, [ogcFid])
  return result.rows.map(({ogc_fid: id, point, camera, buildings, overrides, bearing, google_vision, ...row}, index) => {
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
      ...row
    }
    return result
  })
}

export async function getBuildingsPlaced(client, rangeId) {
  let query = `
WITH
  building_range_info AS (
    SELECT DISTINCT
      a.range_id,
      r_it.tag
    FROM
      rcri_buildings a JOIN iiif r ON
        a.range_id = r.iiif_id
      LEFT JOIN iiif_overrides r_o ON
        r.external_id = r_o.external_id
      LEFT JOIN iiif_overrides_tags r_iot ON
        r_o.iiif_override_id = r_iot.iiif_override_id
      LEFT JOIN iiif_tags r_it ON
        r_iot.iiif_tag_id = r_it.iiif_tag_id
        AND
        r_it.iiif_type_id = 'sc:Range'
  ),
  building_agg AS (
    SELECT
      array_agg(DISTINCT a.range_id) AS range_ids,
      array_agg(DISTINCT a.iiif_id) AS iiif_ids,
      count(DISTINCT b.range_id) AS claimed_count,
      count(DISTINCT c.range_id) AS placed_count,
      a.building_id
    FROM
      rcri_buildings a LEFT JOIN building_range_info b ON
        a.range_id = b.range_id
        AND
        b.tag = 'Claimed'
      LEFT JOIN building_range_info c ON
        a.range_id = c.range_id
        AND
        c.tag = 'Placed'
    GROUP BY
      a.building_id
  )
SELECT DISTINCT
  a.claimed_count,
  a.placed_count,
  a.range_ids,
  a.iiif_ids,
  a.building_id,
  ST_AsGeoJSON(c.wkb_geometry) AS geojson
FROM
  building_agg a JOIN rcri_buildings b ON
    a.building_id = b.building_id
	JOIN lariac_buildings c ON
    a.building_id = c.ogc_fid
`
  const args = []
  if (rangeId) {
    query += 'WHERE b.range_id = $1'
    args.push(rangeId)
  }
  const result = await client.query(query, args)
  return result.rows.map(row => {
    const {
      claimed_count: claimedCount,
      placed_count: placedCount,
      building_id: buildingId,
      range_ids: rangeIds,
      iiif_ids: canvasIds,
      geojson,
    } = row
    return {buildingId, claimedCount, placedCount, rangeIds, canvasIds, geojson: geojson ? JSON.parse(geojson) : null}
  })
}
