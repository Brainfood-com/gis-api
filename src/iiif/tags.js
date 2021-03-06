
export async function getTags(client, iiifId) {
  const bulkTags = await getBulkTags(client, [iiifId])
  return bulkTags[iiifId]
}

export async function getBulkTags(client, iiifIds) {
  const queue = [].concat(iiifIds)
  const result = iiifIds.reduce((result, iiifId) => {
    result[iiifId] = []
    return result
  }, {})

  while (queue.length) {
    const batch = queue.splice(0, 50)
    const query = `
SELECT
  a.iiif_id, b.tag
FROM
  iiif a JOIN iiif_tags b ON
    a.iiif_type_id = b.iiif_type_id
  JOIN iiif_overrides c ON
    a.external_id = c.external_id
  JOIN iiif_overrides_tags d ON
    c.iiif_override_id = d.iiif_override_id
    AND
    b.iiif_tag_id = d.iiif_tag_id
WHERE
  iiif_id IN (${batch.map((item, index) => '$' + (index + 1)).join(', ')})
ORDER BY
  d.sequence_num
`
    const batchResult = await client.query(query, iiifIds)
    batchResult.rows.forEach(row => result[row.iiif_id].push(row.tag))
  }
  return result
}

export async function updateTags(client, iiifId, tags) {
  const query = `
WITH iiif_object AS (
  SELECT iiif_type_id, external_id FROM iiif WHERE iiif_id = $1
), tag_list AS (
  SELECT ROW_NUMBER() OVER() AS sequence_num, tag.tag FROM (SELECT UNNEST($2::text[]) AS tag) tag
), tag_id AS (
  INSERT INTO iiif_tags (iiif_type_id, tag) SELECT iiif_object.iiif_type_id, tag_list.tag FROM iiif_object CROSS JOIN tag_list
  ON CONFLICT (iiif_type_id, tag) DO UPDATE SET tag = EXCLUDED.tag
  RETURNING *
), override_id AS (
  INSERT INTO iiif_overrides (external_id) SELECT iiif_object.external_id FROM iiif_object
  ON CONFLICT (external_id) DO UPDATE SET external_id = EXCLUDED.external_id
  RETURNING iiif_override_id
), delete_ignore AS (
  DELETE FROM iiif_overrides_tags a USING override_id b WHERE a.iiif_override_id = b.iiif_override_id AND a.iiif_tag_id NOT IN (SELECT tag_id.iiif_tag_id FROM tag_id)
  RETURNING a.iiif_override_id
)
INSERT INTO iiif_overrides_tags (iiif_override_id, iiif_tag_id, sequence_num)
SELECT
  c.iiif_override_id, b.iiif_tag_id, a.sequence_num
FROM
  tag_list a JOIN tag_id b ON
    a.tag = b.tag
  CROSS JOIN override_id c LEFT JOIN delete_ignore d ON
    c.iiif_override_id = d.iiif_override_id
ON CONFLICT (iiif_override_id, iiif_tag_id) DO UPDATE SET sequence_num = EXCLUDED.sequence_num
`
  return await client.query(query, [iiifId, tags])
}

export async function searchTags(client, {types, tags}) {
  const query = `
SELECT
  a.iiif_id, a.iiif_type_id, a.label
FROM
  iiif a JOIN iiif_overrides b ON
    a.external_id = b.external_id
  JOIN iiif_overrides_tags c ON
    b.iiif_override_id = c.iiif_override_id
  JOIN iiif_tags d ON
    c.iiif_tag_id = d.iiif_tag_id
WHERE
  a.iiif_type_id IN (${types.map((type, index) => '$' + (index + 1)).join(', ')})
  AND
  LOWER(d.tag) IN (${tags.map((tag, index) => '$' + (index + types.length + 1)).join(', ')})
  `
  const result = await client.query(query, [].concat(types, tags.map(tag => tag.toLowerCase())))
  return result.rows.map(row => row.iiif_id)
}

