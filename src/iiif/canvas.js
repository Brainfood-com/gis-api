import Color from 'color'
import {getTags, updateTags} from './tags'

export function processGoogleVision(rawGoogleVision) {
  if (!rawGoogleVision) {
    return null
  }
  const {
    crop: {
      cropHints,
    } = {},
    image: {
      dominantColors: {
        colors,
      } = {},
    } = {},
    label = [],
    safeSearch = {},
    text = [],
    ...rest
  } = rawGoogleVision

  const labelList = label.map(labelEntry => labelEntry.description)
  const textWordList = text.filter(textEntry => textEntry.description.indexOf('\n') === -1).map(textEntry => textEntry.description)
  const {color: imageColor} = colors.map(({color: {red, green, blue}, score, pixelFraction}) => {
    return {
      color: Color.rgb(red, green, blue),
      score,
      pixelFraction
    }
  }).reduce((result, nextEntry) => {
    const nextScore = result.score + nextEntry.score
    return {
      color: result.color.lighten(result.score / nextScore).mix(nextEntry.color.lighten(nextEntry.score / nextScore)),
      score: nextScore,
      pixelFraction: result.pixelFraction + nextEntry.pixelFraction,
    }
  })
  return {
    labels: labelList,
    ocr: textWordList,
    rgb: imageColor.array(),
    hsv: imageColor.hsl().array(),
    grey: imageColor.grayscale().array(),
  }
}

export async function getOne(client, canvasId) {
  const canvasResult = await client.query("SELECT * FROM canvas WHERE iiif_id = $1", [canvasId])
  const canvasOverrideResult = await client.query("SELECT * FROM canvas_overrides WHERE iiif_id = $1", [canvasId])
  const tags = await getTags(client, canvasId)
  const firstRow = canvasResult.rows[0]
  const firstOverrideRow = canvasOverrideResult.rows[0] || {}
  return {
    id: firstRow.iiif_id,
    label: firstRow.label,
    type: firstRow.iiif_type_id,
    format: firstRow.format,
    height: firstRow.height,
    image: firstRow.image,
    thumbnail: firstRow.thumbnail,
    width: firstRow.width,

    notes: firstOverrideRow.notes,
    exclude: firstOverrideRow.exclude,
    hole: firstOverrideRow.hole,
    tags,

    googleVision: processGoogleVision(firstRow.googleVision),
  }
}

export async function updateOne(client, canvasId, {notes, exclude, hole, tags}) {
  const query = `
WITH canvas_external_id AS (
  SELECT
    external_id
  FROM
    canvas
  WHERE
    iiif_id = $1
), override_id AS (
  INSERT INTO iiif_overrides
    (external_id, notes)
    SELECT
      canvas_external_id.external_id, $2
    FROM canvas_external_id

  ON CONFLICT (external_id) DO UPDATE SET notes = $2
  RETURNING iiif_override_id
)
INSERT INTO iiif_canvas_overrides
  (iiif_override_id, exclude, hole)
  SELECT
    override_id.iiif_override_id, $3, $4
  FROM override_id

  ON CONFLICT (iiif_override_id) DO UPDATE SET (exclude, hole) = ROW($3, $4)
`
  const insertUpdateResult = await client.query(query, [canvasId, notes, exclude, hole])
  await updateTags(client, canvasId, tags)
  return {ok: true}
}

export async function getOverrides(client, iiifOverrideId) {
  const canvasInfo = await client.query('SELECT exclude, hole FROM iiif_canvas_overrides WHERE iiif_override_id = $1', [iiifOverrideId])
  const {exclude, hole} = canvasInfo.rows[0] || {}
  const canvasPointInfo = await client.query('SELECT iiif_canvas_override_source_id, priority, ST_ASGeoJSON(point) point FROM iiif_canvas_point_overrides WHERE iiif_override_id = $1', [iiifOverrideId])
  return {
    exclude,
    hole,
    points: canvasPointInfo.rows.map(row => {
      return {
        source: row.iiif_canvas_override_source_id,
        priority: row.priority,
        point: JSON.parse(row.point),
      }
    }),
  }
}

export async function setOverrides(client, canvasId, {notes, exclude, hole, tags, points}) {
  await exports.updateOne(client, canvasId, {notes, exclude, hole, tags})
  await Promise.all(points.map(({source, priority, point}) => exports.point.updateOne(client, canvasId, source, {priority, point})))
}

export const point = {}
point.nearestEdge = async function nearestEdge(client, point) {
  const query = `
WITH parsed AS (
  SELECT ST_SetSRID(ST_GeomFromGeoJSON($1), 4326) AS point
),
nearest_edge AS (
  SELECT
    gisapp_nearest_edge(parsed.point) AS edge
  FROM
    parsed
)
SELECT
  ST_AsGeoJSON(tl_2017_06037_edges.wkb_geometry) AS edge
FROM
  tl_2017_06037_edges JOIN nearest_edge ON tl_2017_06037_edges.ogc_fid = nearest_edge.edge,
  parsed
`
  const result = await client.query(query, [point])
  if (!result.rowCount) {
    return null
  }
  const {edge} = result.rows[0]
  return {edge: JSON.parse(edge)}
}

point.updateOne = async function updateOne(client, canvasId, sourceId, {priority, point}) {
  const pointAdjustQuery = `
WITH parsed AS (
  SELECT ST_SetSRID(ST_GeomFromGeoJSON($1), 4326) AS point
),
nearest_edge AS (
  SELECT
    gisapp_nearest_edge(parsed.point) AS edge
  FROM
    parsed
)
SELECT
  ST_AsGeoJSON(ST_ClosestPoint(tl_2017_06037_edges.wkb_geometry, parsed.point)) AS point
FROM
  tl_2017_06037_edges JOIN nearest_edge ON tl_2017_06037_edges.ogc_fid = nearest_edge.edge,
  parsed
`
  const adjustResult = await client.query(pointAdjustQuery, [point])
  const adjustedPoint = adjustResult.rowCount ? adjustResult.rows[0].point : point
  const query = `
WITH canvas_external_id AS (
  SELECT
    external_id
  FROM
    canvas
  WHERE
    iiif_id = $1
), override_id AS (
  INSERT INTO iiif_overrides
    (external_id)
    SELECT
      canvas_external_id.external_id
    FROM canvas_external_id

    ON CONFLICT (external_id) DO UPDATE SET external_id = EXCLUDED.external_id
    RETURNING iiif_override_id
)
INSERT INTO iiif_canvas_point_overrides
  (iiif_override_id, iiif_canvas_override_source_id, priority, point)
  SELECT
    override_id.iiif_override_id, $2, $3, ST_SetSRID(ST_GeomFromGeoJSON($4), 4326)
  FROM override_id

  ON CONFLICT (iiif_override_id, iiif_canvas_override_source_id)
  DO UPDATE SET (priority, point) = ROW($3, CASE WHEN $4 IS NOT NULL THEN ST_SetSRID(ST_GeomFromGeoJSON($4), 4326) ELSE NULL END)
`
  const insertUpdateResult = await client.query(query, [canvasId, sourceId, priority, adjustedPoint])
  return {ok: true}
}

point.deleteOne = async function deleteOne(client, canvasId, sourceId) {
  const query = `
WITH override_id AS (
  SELECT
    iiif_override_id
  FROM
    canvas_point_overrides
  WHERE
    iiif_id = $1
)
DELETE FROM iiif_canvas_point_overrides
  USING override_id
  WHERE
    iiif_canvas_point_overrides.iiif_override_id = override_id.iiif_override_id
    AND
    iiif_canvas_override_source_id = $2
`
  const deleteResult = await client.query(query, [canvasId, sourceId])
  return {ok: true}
}
