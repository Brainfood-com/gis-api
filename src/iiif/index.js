import {findAllParents} from '../../server'

export async function detectType(client, context) {
  const iiifResult = await client.query("SELECT iiif_id, iiif_type_id FROM iiif WHERE iiif_id = $1 or external_id = $2", [context.iiifId, context.externalId])
  const firstRow = iiifResult.rows[0]
  const {
    iiif_type_id: iiifTypeId,
    iiif_id: iiifId,
  } = firstRow
  const allParents = await findAllParents(client, iiifTypeId, iiifId)
  return {
    iiifId: iiifId,
    iiifTypeId,
    allParents,
  }
}

