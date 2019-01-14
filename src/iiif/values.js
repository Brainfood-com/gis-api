import {valid as isGeoJSON} from 'geojson-validation'
import isNumber from 'lodash-es/isNumber'

const typeToColumn = {
  NUMBER: 'number_value',
  TEXT: 'text_value',
  TIMESTAMP: 'timestamp_value',
  JSON: 'json_value',
  GEOMETRY: 'geometry_value',
}

const coerceDbToValue = {
  NUMBER(value) {
    return parseFloat(value)
  },
}
export function parseRows(rows) {
  return (rows || []).reduce((result, row) => {
    const {value_type_id: valueTypeId} = row
    const {[valueTypeId]: coerce = value => value} = coerceDbToValue
    result[row.name] = coerce(row[typeToColumn[valueTypeId]])
    return result
  }, {})
}

function valueToRow(value) {
  if (typeof value === 'string') {
    return {value_type_id: 'TEXT', text_value: value}
  } else if (typeof value === 'number') {
    return {value_type_id: 'NUMBER', number_value: value}
  } else if (isGeoJSON(value)) {
    return {value_type_id: 'GEOMETRY', geometry_value: value}
  } else if (typeof value === 'boolean') {
    throw new Error('boolean not supported')
  } else if (value === null) {
    return {value_type_id: 'NULL'}
  } else if (value instanceof Date) {
    return {value_type_id: 'TIMESTAMP', timestamp_value: value}
  } else {
    throw new Error('value not supported:' + value)
  }
}

export async function getValues(client, iiifId) {
  const query = `
SELECT
  c.value_type_id,
  c.number_value,
  c.text_value,
  c.timestamp_value,
  c.json_value,
  c.geometry_value,
  c.name
FROM
  iiif a JOIN iiif_overrides b ON
    a.external_id = b.external_id
  JOIN iiif_overrides_values c ON
    b.iiif_override_id = c.iiif_override_id
WHERE
  iiif_id = $1
ORDER BY
  c.name
`
  const valuesResult = await client.query(query, [iiifId])
  return parseRows(valuesResult.rows)
}

export async function getValue(client, iiifId, name) {
  const query = `
SELECT
  c.value_type_id,
  c.number_value,
  c.text_value,
  c.timestamp_value,
  c.json_value,
  c.geometry_value,
  c.name
FROM
  iiif a JOIN iiif_overrides b ON
    a.external_id = b.external_id
  JOIN iiif_overrides_values c ON
    b.iiif_override_id = c.iiif_override_id
WHERE
  iiif_id = $1
  AND
  name = $2
ORDER BY
  c.name
`
  const valueResult = await client.query(query, [iiifId, name])
  return parseRows(valuesResult.rows)[name]
}

const columnOrder = ['number_value', 'text_value', 'timestamp_value', 'json_value', 'geometry_value']
export async function setValue(client, iiifId, name, value) {
  const valueRow = valueToRow(value)
  const query = `
WITH iiif_object AS (
  SELECT iiif_type_id, external_id FROM iiif WHERE iiif_id = $1
), override_id AS (
  INSERT INTO iiif_overrides (external_id) SELECT iiif_object.external_id FROM iiif_object
  ON CONFLICT (external_id) DO UPDATE SET external_id = EXCLUDED.external_id
  RETURNING iiif_override_id
)
INSERT INTO iiif_overrides_values (iiif_override_id, name, value_type_id, number_value, text_value, timestamp_value, json_value, geometry_value)
SELECT
  a.iiif_override_id, $1, $2, $3, $4, $5, $6, $7
FROM
  override_id a
ON CONFLICT (iiif_override_id, name) DO UPDATE SET
  value_type_id = EXCLUDED.value_type_id,
  number_value = $3,
  text_value = $4,
  timestamp_value = $5,
  json_value = $6,
  geometry_value = $7
`
  return await client.query(query, [iiifId, name, ...columnOrder.map(column => valueRow[column])])
}

export async function updateValues(client, iiifId, values = {}) {
  const valuesList = Object.entries(values).map(([name, value]) => ({name, ...valueToRow(value)}))
  const query = `
WITH iiif_object AS (
  SELECT iiif_type_id, external_id FROM iiif WHERE iiif_id = $1
), value_list AS (
  SELECT * FROM json_populate_recordset(null::iiif_overrides_values, $2)
), override_id AS (
  INSERT INTO iiif_overrides (external_id) SELECT iiif_object.external_id FROM iiif_object
  ON CONFLICT (external_id) DO UPDATE SET external_id = EXCLUDED.external_id
  RETURNING iiif_override_id
), delete_ignore AS (
  DELETE FROM iiif_overrides_values a USING override_id b WHERE a.iiif_override_id = b.iiif_override_id AND a.name NOT IN (SELECT name FROM value_list)
  RETURNING a.iiif_override_id
)
INSERT INTO iiif_overrides_values (iiif_override_id, name, value_type_id, number_value, text_value, timestamp_value, json_value, geometry_value)
SELECT
  b.iiif_override_id, a.name, a.value_type_id,
  a.number_value,
  a.text_value,
  a.timestamp_value,
  a.json_value,
  a.geometry_value
FROM
  value_list a CROSS JOIN override_id b
  LEFT JOIN delete_ignore c ON
    b.iiif_override_id = c.iiif_override_id
ON CONFLICT (iiif_override_id, name) DO UPDATE SET
  value_type_id = EXCLUDED.value_type_id,
  number_value = EXCLUDED.number_value,
  text_value = EXCLUDED.text_value,
  timestamp_value = EXCLUDED.timestamp_value,
  json_value = EXCLUDED.json_value,
  geometry_value = EXCLUDED.geometry_value
`
  return await client.query(query, [iiifId, JSON.stringify(valuesList)])
}

export async function deleteValue(client, iiifId, name) {
  const query = `
DELETE FROM iiif_overrides_values
WHERE
  iiif_override_id = (SELECT a.iiif_override_id FROM iiif_overrides a JOIN iiif b ON a.external_id = b.external_id) WHERE b.iiif_id = $1)
  AND
  name = $2
`
  return await client.query(query, [iiifId, name])
}
















