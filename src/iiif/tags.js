
export async function getTags(client, iiifId) {
  const query = `
SELECT
  b.tag
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
  iiif_id = $1
ORDER BY
  d.sequence_num
`
  const tagsResult = await client.query(query, [iiifId])
  return tagsResult.rows.map(row => row.tag)
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
  DELETE FROM iiif_overrides_tags USING tag_id, override_id WHERE iiif_overrides_tags.iiif_override_id = override_id.iiif_override_id AND iiif_overrides_tags.iiif_tag_id NOT IN (tag_id.iiif_tag_id)
)
INSERT INTO iiif_overrides_tags (iiif_override_id, iiif_tag_id, sequence_num)
SELECT
  c.iiif_override_id, b.iiif_tag_id, a.sequence_num
FROM
  tag_list a JOIN tag_id b ON
    a.tag = b.tag
  CROSS JOIN override_id c
ON CONFLICT (iiif_override_id, iiif_tag_id) DO UPDATE SET sequence_num = EXCLUDED.sequence_num
`
  return await client.query(query, [iiifId, tags])
}

