
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

export async function getBuildingCanvases(client, ogcFid) {
  let query = `
SELECT
  b.ogc_fid,
  c.bearing,
  ST_AsGeoJSON(c.point) AS point,
  ST_AsGeoJSON(c.camera) AS camera,
  d.*
FROM
  lariac_buildings b JOIN routing_canvas_range_interpolation_cache c ON
    ST_Intersects(c.camera, b.wkb_geometry)
  JOIN range_canvas d ON
    c.range_id = d.range_id
    AND
    c.iiif_id = d.iiif_id
WHERE
  b.ogc_fid = $1
ORDER BY
  d.range_id, d.sequence_num
`.replace(/[\t\r\n ]+/g, ' ')
  const result = await client.query(query, [ogcFid])
  return result.rows.map(({ogc_fid: id, camera, point, google_vision, ...rest}) => {
    return {
      id,
      point: point ? JSON.parse(point) : undefined,
      camera: camera ? JSON.parse(camera) : undefined,
      googleVision: processGoogleVision(google_vision),
      ...rest,
    }
  })
}
