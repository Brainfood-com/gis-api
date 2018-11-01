import {getTags, updateTags} from './tags'

const type = 'sc:Collection'

export async function getAll(client) {
  const collectionResult = await client.query("SELECT iiif_id, label FROM iiif WHERE iiif_type_id = $1", [type])
  return collectionResult.rows.map(row => {
    return {id: row.iiif_id, label: row.label, type}
  })
}

export async function getOne(client, collectionId) {
  const collectionResult = await client.query("SELECT iiif_id, label FROM iiif WHERE iiif_type_id = $1 AND iiif_id = $2", [type, collectionId])
  const collectionOverrideResult = await client.query("SELECT * FROM collection_overrides WHERE iiif_id = $1", [collectionId])
  const firstRow = collectionResult.rows[0]
  const firstOverrideRow = collectionOverrideResult.rows[0] || {}
  const assocResult = await client.query("SELECT a.iiif_id_to, a.iiif_assoc_type_id, b.label, b.iiif_type_id FROM iiif_assoc a JOIN iiif b ON a.iiif_id_to = b.iiif_id WHERE a.iiif_id_from = $1 ORDER BY a.sequence_num, b.label", [collectionId])
  const tags = await getTags(client, collectionId)
  return {
    id: firstRow.iiif_id,
    label: firstRow.label,
    manifests: await Promise.all(assocResult.rows.map(async row => {
      const manifestId = row.iiif_id_to
      const tags = await getTags(client, manifestId)
      return {id: manifestId, type: row.iiif_assoc_type_id, label: row.label, type: row.iiif_type_id, tags}
    })),
    type,
    notes: firstOverrideRow.notes,
    tags,
  }
}

export async function updateOne(client, collectionId, {notes, tags}) {
  const query = `
WITH collection_external_id AS (
  SELECT
    external_id
  FROM
    collection
  WHERE
    iiif_id = $1
)
INSERT INTO iiif_overrides
  (external_id, notes)
  SELECT
    collection_external_id.external_id, $2
  FROM collection_external_id

  ON CONFLICT (external_id) DO UPDATE SET notes = $2
`
  const insertUpdateResult = await client.query(query, [collectionId, notes])
  await updateTags(client, collectionId, tags)
  return {ok: true}
}

export const setOverrides = updateOne

export async function getOverrides(client, iiifOverrideId) {
  return {}
}
