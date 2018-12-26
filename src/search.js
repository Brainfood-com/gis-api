
import {gmaps_geocode} from './google/gmaps-geocode'

function boxToPolygon(box) {
  if (!box) {
    return undefined
  }
  const {northeast, southwest} = box
  return {
    type: 'Polygon',
    coordinates: [
      [
        [northeast.lng, northeast.lat],
        [southwest.lng, northeast.lat],
        [southwest.lng, southwest.lat],
        [northeast.lng, southwest.lat],
        [northeast.lng, northeast.lat],
      ],
    ],
  }
}

export async function search(client, searchContext) {
  const {address} = searchContext


  // Limit lookup to the region of photos that have been placed
  const boundsResult = await client.query('SELECT ST_AsGeoJSON(ST_Extent(geometry)) AS global_bounds FROM rcri_range_summary WHERE name = $1', ['global_bounds'])
  const bounds = JSON.parse(boundsResult.rows[0].global_bounds).coordinates[0].reduce((result, point) => {
    return {
      ne: {
        lng: Math.max(point[0], result.ne.lng),
        lat: Math.max(point[1], result.ne.lat),
      },
      sw: {
        lng: Math.min(point[0], result.sw.lng),
        lat: Math.min(point[1], result.sw.lat),
      },
    }
  }, {
    ne: {lng: -180, lat: -90},
    sw: {lng: 180, lat: 90},
  })
  const addressResults = await gmaps_geocode(address, bounds)


  //(SELECT array_agg(ogc_fid) FROM lariac_buildings WHERE ST_Intersects(parsed.camera, wkb_geometry)) AS buildings,
  const query = `
WITH parsed AS (
  SELECT ST_Buffer(ST_SetSRID(ST_GeomFromGeoJSON($1), 4326), 0.0004) AS target
)
SELECT
  ST_AsGeoJSON(a.target) AS search_area,
  array_agg(b.ogc_fid) AS building_ids
FROM
  parsed a LEFT JOIN lariac_buildings b ON
    ST_Intersects(a.target, b.wkb_geometry)
GROUP BY
  a.target
`
  const buildingIds = {}
  const rangeToCanvases = {}
  const canvases = {}

  const addresses = await Promise.all(addressResults.map(async addressResult => {
    const {
      placeId,
      formattedAddress,
      geometry: {
        bounds,
        location,
        locationType,
        viewport,
      },
    } = addressResult
    const geolocation = {type: 'Point', coordinates: [location.lng, location.lat]}

               const queryResult = await client.query(query, [JSON.stringify(geolocation)])
    const {search_area, building_ids} = queryResult.rows[0]
    return {
      placeId,
      formattedAddress,
      searchArea: JSON.parse(search_area),
      geolocation,
      locationType,
      bounds: boxToPolygon(bounds),
      viewport: boxToPolygon(viewport),
      buildingIds: building_ids,
    }
  }))
  return addresses
}

