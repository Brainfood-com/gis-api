import fetch from 'node-fetch'
import {URL, URLSearchParams} from 'url'

export async function gmaps_geocode(address, bounds) {
  const {GOOGLE_MAPS_API_KEY} = process.env
  const url = new URL('https://maps.googleapis.com/maps/api/geocode/json')
  url.search = new URLSearchParams({
    key: GOOGLE_MAPS_API_KEY,
    address,
    bounds: `${bounds.sw.lat},${bounds.sw.lng}|${bounds.ne.lat},${bounds.ne.lng}`,
  }).toString()
  const result = await fetch(url.toString()).then(data => data.json())
  return result.results.map(addressResult => {
    const {
      place_id: placeId,
      formatted_address: formattedAddress,
      geometry: {
        bounds,
        location,
        location_type: locationType,
        viewport,
      },
    } = addressResult
    return {
      placeId,
      formattedAddress,
      geometry: {
        bounds,
        location,
        locationType,
        viewport,
      },
    }
  })
}

