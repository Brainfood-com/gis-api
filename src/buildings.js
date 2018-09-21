

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
    return null
  }
  query += ` a.ogc_fid IN (${ogcFids.map((id, index) => `$${index + 1}`).join(', ')})`
  const result = await client.query(query, ogcFids)
  return result.rows.map(({ogc_fid: id, geojson, ...rest}) => {
    return {...rest, id, geojson: geojson ? JSON.parse(geojson) : null}
  })
}
