import {getTags, getBulkTags, updateTags} from './tags'
import * as iiifValues from './values'

export async function getParents(client, manifestId) {
  const dbResult = await client.query("SELECT DISTINCT iiif_id_from FROM iiif_assoc WHERE iiif_id_to = $1 AND iiif_assoc_type_id = 'sc:Manifest'", [manifestId])
  return dbResult.rows.map(row => ['sc:Collection', row.iiif_id_from])
}

export async function getOne(client, manifestId) {
  const manifestResult = await client.query("SELECT * FROM manifest WHERE iiif_id = $1", [manifestId])
  const manifestOverrideResult = await client.query("SELECT * FROM manifest_overrides WHERE iiif_id = $1", [manifestId])
  const tags = await getTags(client, manifestId)
  const firstRow = manifestResult.rows[0]
  const firstOverrideRow = manifestOverrideResult.rows[0] || {}
  return {
    id: firstRow.iiif_id,
    attribution: firstRow.attribution,
    description: firstRow.description,
    externalId: firstRow.external_id,
    label: firstRow.label,
    license: firstRow.license,
    logo: firstRow.logo,
    type: firstRow.iiif_type_id,
    viewingHint: firstRow.viewingHint,
    notes: firstOverrideRow.notes,
    tags,
  }
}

export async function updateOne(client, manifestId, {notes, tags}) {
  const query = `
WITH manifest_external_id AS (
  SELECT
    external_id
  FROM
    manifest
  WHERE
    iiif_id = $1
)
INSERT INTO iiif_overrides
  (external_id, notes)
  SELECT
    manifest_external_id.external_id, $2
  FROM manifest_external_id

  ON CONFLICT (external_id) DO UPDATE SET notes = $2
`
  const insertUpdateResult = await client.query(query, [manifestId, notes])
  await updateTags(client, manifestId, tags)
  return {ok: true}
}
export const setOverrides = updateOne

export async function getOverrides(client, iiifOverrideId) {
  return {}
}

export async function getStructures(client, manifestId) {
  const manifestValues = await iiifValues.getValues(client, manifestId)
  const manifestYear = manifestValues.year
  const manifestRangesResult = await client.query(`
SELECT
  a.iiif_id_to AS range_id,
  b.external_id,
  b.iiif_type_id,
  b.label,
  c.viewing_hint,
  d.values
FROM
  iiif_assoc a JOIN iiif b ON
    a.iiif_id_to = b.iiif_id
    AND
    a.iiif_assoc_type_id = 'sc:Range'
  JOIN iiif_range c ON
    b.iiif_id = c.iiif_id
  LEFT JOIN iiif_values d ON
    b.iiif_id = d.iiif_id
WHERE
  a.iiif_id_from = $1
ORDER BY
  a.sequence_num,
  b.label
`, [manifestId])
  const manifestRangeMembersResult = await client.query(`
WITH has_point_override AS (
  SELECT
    a.external_id,
    a.iiif_override_id
  FROM
    iiif_overrides a JOIN iiif_canvas_point_overrides b ON
      a.iiif_override_id = b.iiif_override_id
  WHERE
    b.point IS NOT NULL
)
SELECT
  a.iiif_id_to AS range_id,
  b.iiif_assoc_type_id,
  b.iiif_id_to AS member_id,
  c.iiif_type_id AS member_type_id,
  EXISTS(SELECT * FROM has_point_override WHERE has_point_override.external_id = c.external_id) AS has_override_point
FROM
  iiif_assoc a JOIN iiif_assoc b ON
    a.iiif_id_to = b.iiif_id_from
    AND
    a.iiif_assoc_type_id = 'sc:Range'
  JOIN iiif c ON
    b.iiif_id_to = c.iiif_id
WHERE
  a.iiif_id_from = $1
ORDER BY
  b.sequence_num
`, [manifestId])
  const rangesToMembers = {}
  manifestRangeMembersResult.rows.forEach(memberRow => {
    const {range_id: rangeId, iiif_assoc_type_id: typeId, member_id: memberId, member_type_id: memberTypeId, has_override_point: hasOverridePoint} = memberRow
    const rangeMembers = rangesToMembers[rangeId] || (rangesToMembers[rangeId] = {ranges: [], canvases: [], pointOverrideCount: 0})
    if (hasOverridePoint) {
      ++rangeMembers.pointOverrideCount
    }
    switch (typeId) {
      case 'sc:Range':
        rangeMembers.ranges.push({id: memberId, type: memberTypeId})
        break
      case 'sc:Canvas':
        rangeMembers.canvases.push({id: memberId, type: memberTypeId})
        break
    }
  })
  const allRangeIds = manifestRangesResult.rows.map(row => row.range_id)
  const allRangeTags = await getBulkTags(client, allRangeIds)
  const structures = manifestRangesResult.rows.map(rangeRow => {
    const {range_id: rangeId, external_id: externalId, label, iiif_type_id: type, viewing_hint: viewingHint, values} = rangeRow
    const members = rangesToMembers[rangeId] || {}
    const tags = allRangeTags[rangeId]
    return {...members, id: rangeId, externalId, label, type, viewingHint, tags, manifestYear, values: iiifValues.parseRows(values)}
  })
  return structures
}
