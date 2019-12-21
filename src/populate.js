const orbitDbApi = require('./orbit-db-client.js')
const torrents = require('../torrents.json')

const dbAddr = '%2Forbitdb%2FzdpuAu6VkBB93Pb2YpXU7cdT1XmBjRy8HpEyNXbqsYS7kDCT9%2Flib-genesis-test4'
const dbServer = 'https://orbitdb-api.phillm.net:3000'




async function  populate() {

  const existing = (await orbitDbApi.get(dbServer, 'db/' + dbAddr + '/all')).map(t=>t._id)
  const missing = torrents.filter((t=> !(existing.includes(t._id))))
  console.log(missing, missing.length)
  for (const t of missing) {
    await orbitDbApi.post(dbServer, `db/${dbAddr}/put`, t)
  }
  process.exit()
}

populate()

