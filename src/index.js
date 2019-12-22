const Client = require('bittorrent-tracker')
const DHT =  require('bittorrent-dht')
const crypto = require('crypto');
const orbitDbApi = require('./orbit-db-client.js')

const dbServer = process.env.ORBITDB_API_SERVER
const dbAddr = process.env.ORBIT_DB_ADDR

const scraperCount = parseInt(process.env.LIBGEN_SCRAPER_COUNT)
const scraperID = parseInt(process.env.LIBGEN_SCRAPER_ID)

const max_stale = parseInt(process.env.MAX_STALE)

if(!dbServer) {
    throw Error('ORBITDB_API_SERVER is required')
}

if(!dbAddr) {
    throw Error('ORBIT_DB_ADDR is required')
}

if(!max_stale) {
    throw Error('MAX_STALE is required')
}

if(isNaN(scraperCount)) {
    throw new Error('LIBGEN_SCRAPER_COUNT is required')
}

if(isNaN(scraperID)) {
    throw new Error('LIBGEN_SCRAPER_ID is required')
}

console.info({scraperCount, scraperID, max_stale})

orbitDbApi.post(dbServer, 'db/' + dbAddr, {awaitOpen: false})


function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function scrapeAll(torrents) {
    const finished = {}

    const hashes = Object.keys(torrents)

    console.debug('Scraping...')

    while (!(() => {
        return hashes.every(h => h in finished)
    })()) {
        const toScrape = hashes.filter(h => !(h in(finished)))
        console.debug(`Queue length ${toScrape.length}`)
        for (const infoHash of toScrape) {
            const torrent = torrents[infoHash]
            console.debug(`Scraping ${Object.keys(finished).length} of ${hashes.length}: ${infoHash}`)
            if(isStaleDHT(torrent)) {
                await scrapeDHT(torrent).catch(err => console.error(err))
            } else {
                console.info('Skipping DHT scrape')
            }
            await scrapeTrackers(torrent)

            console.debug(torrent)
            if (!isStale(torrent)) {
                orbitDbApi.post(dbServer, `db/${dbAddr}/put`, torrent)
                finished[infoHash] = torrent
            }
            await sleep(1000)
        }
    }

    console.debug(`Got ${Object.keys(finished).length} results`)
}

function scrapeDHT(torrent) {
    return new Promise((resolve, reject ) => {
        console.debug('Scraping DHT peers ...')
        try {
            let dht = new DHT()
            const peers = []
            dht.on('peer', function (peer, _ih, _from) {
                peerhash = crypto.createHash('md5').update(peer.host + peer.port).digest('hex');
                if (!(peers.includes(peerhash))){
                    peers.push(peerhash)
                }
            })
            dht.lookup(torrent._id, async function() {
                console.debug('DHT scrape complete')
                torrent.dhtData = {
                    infoHash: torrent._id,
                    peers: peers.length,
                    scraped_date: Math.floor(new Date() / 1000)
                }
                resolve()
            })
        } catch (err) {
            reject(err)
        }
    })
}

async function scrapeTrackers(torrent) {
    const trackerData = torrent.trackerData || {}
    const trackers = torrent.trackers.filter((tracker) => isStaleTracker(torrent, tracker))
        for(const announce of trackers) {
            try {
                console.debug(`Scraping tracker ${announce} ...`)
                trackerData[announce] = await scrape(torrent._id, announce)
            } catch (err) {
                console.error(err)
            }
        }
        torrent.trackerData = trackerData
}

function scrape(infoHash, announce) {
    return new Promise((resolve, reject) => {
        Client.scrape({infoHash, announce}, (err,data) => {
            console.debug(`Scraped ${announce}`)
            if (err) {
                reject(err)
            } else {
                data.scraped_date = Math.floor(new Date() / 1000)
                resolve(data)
            }
        })
    })
}

function isStale(torrent) {
    if(torrent.trackers.length == 0) {
        console.warn(`${torrent._id} has no trackers`)
    }

    if(!(torrent.trackerData)){
        return true
    }

    if(isStaleDHT(torrent)) {
        return true
    }

    for (const tracker of torrent.trackers) {
        if (isStaleTracker(torrent, tracker)) {
            return true
        }
    }
    return false
}

function isStaleTracker(torrent, tracker) {
    if(!(torrent.trackerData)){
        return true
    }

    if (!(torrent.trackerData[tracker])) {
        return true
    }

    if(torrent.trackerData[tracker].scraped_date + max_stale < Math.floor(new Date() / 1000)) {
        return true
    }
    return false
}

function isStaleDHT(torrent) {
    if (!(torrent.dhtData)) {
        return true
    }

    if(torrent.dhtData.scraped_date + max_stale < Math.floor(new Date() / 1000)) {
        return true
    }
}

async function run() {
    if(!lockOut) {
        lockOut = true
        let appendTrackers = []
        if (process.env.TRACKERS_FILE) {
            appendTrackers = require(process.env.TRACKERS_FILE)
            console.debug(`Loaded ${appendTrackers.length} trackers`)

        }

        const torrents = {}
        const updates = {}

        if(process.env.UPDATES_FILE) {
            for(const t of require(process.env.UPDATES_FILE)) {
                updates[t._id] = t
            }
            console.debug(`Loaded ${Object.keys(updates).length} updates`)
        }


        try {
            const loaded = (await orbitDbApi.get(dbServer, `db/${dbAddr}/all`)).sort(function (a, b) {
            if (a._id > b._id) {
                return 1;
            }
            if (b._id > a._id) {
                return -1;
            }
            return 0;
            })
            console.debug(`Loaded ${Object.keys(loaded).length} torrents`)

            const splitLength = Math.ceil(Object.keys(loaded).length / scraperCount);

            const minTorrents = splitLength * (scraperID - 1)
            const maxTorents = splitLength * scraperID

            console.debug(`Checking torrents ${minTorrents} to ${maxTorents}`)

            for (const t of Object.values(loaded).slice(minTorrents, maxTorents)) {
                if(!t.trackers) {
                    t.trackers = []
                }

                if(isStale(t)) {
                    try{

                        for (const append of appendTrackers) {
                            if (!(t.trackers.includes(append))) {
                                t.trackers.push(append)
                                console.debug(`Added tracker ${append}`)
                            }
                        }

                        if(Object.keys(updates).length > 0) {
                            if((!(t.name)) || (t.name !== updates[t._id].name)) {
                                t.name = updates[t._id].name
                            }

                            if((!t.link) || (t.link !== updates[t._id].link)) {
                                t.link = updates[t._id].link
                            }

                            if((!t.type) || (t.type !== updates[t._id].type)) {
                                t.type = updates[t._id].type
                            }

                            if((!t.size_bytes) || (t.size_bytes !== updates[t._id].size_bytes)) {
                                t.size_bytes = updates[t._id].size_bytes
                            }
                            if((!t.created_unix) || t.created_unix !== updates[t._id].created_unix) {
                                t.created_unix = updates[t._id].created_unix
                            }
                        }
                    } catch(err) {
                        console.debug(t)
                        console.error(err)
                        process.exit()
                    }

                    torrents[t._id] = t
                    console.debug(t)
                }
            }

            if (Object.keys(torrents).length > 0) {
                await scrapeAll(torrents)
            } else {
                console.debug('No stale torrents')
            }
        } catch (err) {
            console.error(err)
        }
        console.info(new Date())
        lockOut = false

    } else {
        console.debug('Already running')
    }

}

let lockOut = false

run()
setInterval(run, 300 * 1000)

