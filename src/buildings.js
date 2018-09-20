

export async function getBuildings(client, ...ogcFids) {
  let query = `
SELECT DISTINCT
  ogc_fid, code, bld_id, height, elev, area, source, date_, ain, shape_leng, shape_area,
  ST_AsGeoJSON(wkb_geometry) AS geojson
FROM
  lariac_buildings
WHERE
`.replace(/[\t\r\n ]+/g, ' ')
  if (!ogcFids.length) {
    return null
  }
  query += ` ogc_fid IN (${ogcFids.map((id, index) => `$${index + 1}`).join(', ')})`
  const result = await client.query(query, ogcFids)
  return result.rows.map(({ogc_fid: id, geojson, ...rest}) => {
    return {...rest, id, geojson: geojson ? JSON.parse(geojson) : null}
  })
}

