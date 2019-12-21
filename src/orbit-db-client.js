const http2 = require('http2');

const clients = {}

const getClient = (url) => {
    if(url in clients) {
        if(clients[url].destroyed) {
            console.debug('Session destroyed')
            delete clients[url]
        }

        if(clients[url].closed) {
            console.debug('Session closed')
            delete clients[url]
        }
    }
    if (!(url in clients)) {
        clients[url] = http2.connect(url);
        clients[url].once('goaway', function () {
            console.debug('goaway')
            delete clients[url]
        })
        clients[url].once('error', function () {
            console.debug('error')
            delete clients[url]
        })
        clients[url].once('timeout', function () {
            console.debug('timeout')
            delete clients[url]
        })
    }
    return clients[url];
}

const get = (url, path) => new Promise((resolve, reject) => {
    let errored = false

    const client = getClient(url);

    const req = client.request({
        [http2.constants.HTTP2_HEADER_SCHEME]: "https",
        [http2.constants.HTTP2_HEADER_METHOD]: http2.constants.HTTP2_METHOD_GET,
        [http2.constants.HTTP2_HEADER_PATH]: `/${path}`
    });

    req.setEncoding('utf8');
    let body = ''
    req.on('data', (chunk) => {
        body += chunk;
    });
    req.end();
    req.once('end', () => {
        if (body === '') {
            reject(new Error('Empty response'))
        }
        try{
            resolve(JSON.parse(body));
        } catch (err) {
            errored = true
            console.error(err)
            reject(err)
        }
    });
    req.once('error', (err) =>reject(err))
  });

const post = (url, path, body) => new Promise((resolve, reject) => {
    let errored = false

    const client = getClient(url);

    const buffer = new Buffer.from(JSON.stringify(body));

    const req = client.request({
        [http2.constants.HTTP2_HEADER_SCHEME]: "https",
        [http2.constants.HTTP2_HEADER_METHOD]: http2.constants.HTTP2_METHOD_POST,
        [http2.constants.HTTP2_HEADER_PATH]: `/${path}`,
        "Content-Type": "application/json",
        "Content-Length": buffer.length,
    });

    req.setEncoding('utf8');
    let rbody = ''
    req.on('data', (chunk) => {
        rbody += chunk;
    });
    req.write(buffer);
    req.end();

    req.once('end', () => {
        if (rbody === '') {
            reject(new Error('Empty response'))
            errored = true
        }
        if(!errored) {
            try{
                resolve(JSON.parse(rbody));
            } catch (err) {
                console.debug(`rbody: '${rbody}'`)
                console.error(err)
                process.exit()
            }
        } else {
            console.warn('Errored already')
        }
    });

    req.once('error', (err) => {
        errored = true
        console.error(err)
        reject(err)
    })
    req.once('frameError', (err) => {
        errored = true
        console.error(err)
        reject(err)
    })
  });



  module.exports = {get, post}