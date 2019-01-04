import { Client, Pool } from 'pg'
import promiseLimit from 'promise-limit'
import getenv from 'getenv'

const dbConf = {
	user: 'gis',
	host: 'postgresql',
	database: 'gis',
	password: 'sig',
}
const pool = new Pool({
  ...dbConf,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
})
pool.on('connect', client => {
  client.on('notification', msg => {
    console.log(`NOTIFICATION: ${msg.channel}: ${msg.payload}`)
  })
  client.on('notice', msg => {
    const {notice} = msg
    if (true || notice) {
      console.log('NOTICE:', '' + msg)
    }
  })
})

const dbConnectionLimit = promiseLimit(getenv.int('DB_CONNECTION_LIMIT', 10))

//    this._pgConnection = new Client({

async function dbPoolConnection(handler) {
  const client = await pool.connect()
  try {
    return await handler(client)
  } finally {
    client.release()
  }
}

class LimitedPoolProxy {
  query(...args) {
    return this.run(client => client.query(...args))
  }

  run(handler) {
    return dbConnectionLimit(() => dbPoolConnection(handler))
  }
}

const limitedPoolProxy = new LimitedPoolProxy()

export async function dbPoolWorker(handler) {
  return handler(limitedPoolProxy)
}

export function wrapForTransaction(handler) {
  return async (client, ...rest) => {
    console.log('BEGIN')
    await client.query('BEGIN')
    let r
    try {
      r = await handler(client, ...rest)
    } catch (e) {
      console.log('ROLLBACK')
      await client.query('ROLLBACK')
      throw e
    }
    console.log('COMMIT')
    await client.query('COMMIT')
    return r
  }
}


