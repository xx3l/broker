'use strict'

const express = require('express');
const httpBodyParser = require('body-parser');
const fs = require('fs');
const https = require('https');
const cookieParser = require('cookie-parser');

class BrokerClass {
    #httpServer;
    #httpsServer;
    #queues;
    config = {};

    constructor() {
        this.configure({
            dumpFile: './state/dump.json',
            debug: 1,
        })
        this.setDebugHandler();
        this.#httpServer = false;
        this.#httpsServer = false;
        this.version = require('./package.json').version;
        this.debug(1, `[Info] Broker ${this.version} started`);
        this.clear();
        let data = {queues: {}, caches: {}}
        try {
            data = JSON.parse(fs.readFileSync(this.config.dumpFile, {encoding: 'utf8', flag: 'r'}));
            this.debug(2, `[Event] dump ${this.config.dumpFile} parsed for state restoring`);
        } catch (e) {
            data = {queues: {}, caches: {}}
            this.debug(1, `[Warn] dump can't be used for restoring`);
            return;
        }
        for (const queue of Object.keys(data.queues || {})) {
            this.createQueue(queue)
            this.queue[queue].messages = data.queues[queue].messages
            this.queue[queue].broker = this;
            this.debug(2, `[Event] Queue ${queue} loaded with ${queue.length} elements`);
        }
        for (const cache of Object.keys(data.caches || {})) {
            this.createCache(cache, 0)
            this.cache[cache].data = data.caches[cache].data
            this.cache[cache].broker = this;
            this.debug(2, `[Event] Cache ${cache} loaded with ${cache.size} elements`);
        }
        this.debug(1, 'broker state restored');
    }

    configure(config = {}) {
        Object.keys(config).map(key => {
            this.config[key] = config[key]
        });
        return this;
    }

    debug(level, ...data) {
        if (this.config.debug >= level) {
            this.debugHandler(...data);
        }
    }

    setDebugHandler(handler = (...messages) => console.log(...messages)) {
        this.debugHandler = handler;
    }

    setDebugLevel(level) {
        this.config.debug = level;
        return this;
    }

    clear() {
        this.queue = {};
        this.#queues = [];
        this.cache = {};
        this.caches = [];
    }

    configureHTTP(config = { port: 3000 }) {
        if (!this.#httpServer) {
            this.httpServerPort = config?.port || 3000;
            this.#httpServer = express();
            this.#httpServer
                .set("view engine", config.engine || "pug")
                .set('views', 'webroot')
            this.#httpServer.use(httpBodyParser.urlencoded({extended: true}));
            this.#httpServer.use(httpBodyParser.json());
            this.#httpServer.use(httpBodyParser.raw());
            this.#httpServer.use(cookieParser());


            this.#httpServer.get('/broker', (req, res) => {
                res.render('index', {broker: this});
            });
        }
        return this;
    }
    configureHTTPS(config = { port: 3001, credentials: {}}) {
        const credentials = {
            cert: config.credentials.cert || fs.readFileSync(config.credentials.certFile),
            key: config.credentials.key  || fs.readFileSync(config.credentials.keyFile)
        }
        if (!this.#httpsServer) {
            this.httpsServerPort = config?.port || 3001;
            this.#httpsServer = https.createServer(credentials, this.#httpServer);
        }
        return this;
    }

    startHttp(mode = 'dev') {
        const signals = [`SIGINT`, `SIGUSR1`, `SIGUSR2`, `SIGTERM`];
        if (mode != 'dev') {
            signals.push('uncaughtException');
        }
        signals.forEach(event => {
            process.on(event, () => {
                Broker.sync()
                process.exit(-1);
            });
        })
        this.start();
        if (this.#httpsServer) {
            this.debug(1, `[Info] HTTPS started at ::${this.httpsServerPort}`);
            this.#httpsServer.listen(this.httpsServerPort);
        }
        else {
            this.debug(1, `[Info] HTTP started at ::${this.httpServerPort}`);
            this.#httpServer.listen(this.httpServerPort);
        }
    }

    start() {
        this.#queues.forEach((queue) => queue.start());
        return this;
    }

    createQueue(name) {
        if (this.queue[name]) {
            return this.queue[name];
        }
        const newQueue = new class {
            constructor() {
                this.in = null;
                this.out = null;
                this.length = 0;
                this.messages = []
                this.allowTicks = true;
                this.inputAttached = false;
                this.handlerAttached = false;
                this.alive = false;
                this.tick = this.tick.bind(this);
                this.tickPointer = null;
                this.busy = false;
                this.debug = true;
            }

            write(message, params = {}) {
                const msg = {
                    priority: params.priority || 0,
                    created: Date.now(),
                    state: params.state || 'input',
                    data: {...message}
                }
                this.messages.push(msg);
                this.length = this.messages.length;
                return msg;
            }

            read() {
                const message = this.messages.shift();
                this.length = this.messages.length;
                return message;
            }

            attachInputWeb(route, prePushAction, responseAction, tickInterval = 1000) {
                this.allowTicks = true;
                this.inputAttached = true;
                this.setHandler(() => {
                }, tickInterval)
                this.broker.#httpServer.all(route, (req, res) => {
                    if (!prePushAction) {
                        prePushAction = (req) => {
                            return {
                                url: req.url,
                                query: req.query,
                                body: req.body,
                                cookies: req.cookies,
                            }
                        }
                    }
                    const value = prePushAction(req, this);
                    if (value) {
                        const msg = this.write(value);
                        msg.state = 'processed';
                    }
                    if (responseAction.constructor.name === "AsyncFunction") {
                        responseAction(req, this).then(result => res.send(result));
                    } else {
                        res.send(responseAction(req, this));
                    }

                });
                return this;
            }

            attachInput(queue) {
                this.in = queue;
                queue.out = this;
                this.inputAttached = true;
                return this;
            }

            async tick() {
                if (!this.alive) return;
                if (this.busy) return;
                this.busy = true;
                if (this.handlerAttached) {
                    this.broker.debug(3, `[Event] tick handling on Queue::${this.name} {len:${this.length}}`);
                    await this.messages.map(async (message) => {
                        if (message.state != 'processed') {
                            this.broker.debug(3, `[Event] tick handling message Queue::${this.name}->message(${JSON.stringify(message)})`);
                            this.handler(message, this);
                            message.state = 'processed';
                        }
                    })
                }
                const lastMsg = this.read();
                if (this.out && lastMsg) {
                    this.broker.debug(3, `[Event] Queue::${this.name} -> Queue::${this.out.name}`, lastMsg);
                    this.out.write(lastMsg.data);
                }
                this.busy = false;
            }

            start() {
                this.broker.debug(2, `[Event] Queue::${this.name} started`);
                this.alive = true;
                this.tickPointer = setInterval(this.tick, this.tickInterval);
            }

            stop() {
                this.alive = false;
                clearInterval(this.tickPointer);
            }

            setHandler(handler, tickInterval = 1000) {
                this.tickInterval = tickInterval;
                this.handlerAttached = true;
                this.handler = handler;
            }

            getDelayMax() {
                const now = Date.now();
                const oldest = this.messages.reduce((a, b) => b.created < a ? b.created : a, now)
                return now - oldest;
            }
        }
        newQueue.broker = this;
        newQueue.name = name;
        this.queue[name] = newQueue;
        this.#queues.push(newQueue);
        return newQueue;
    }

    createCache(name, purgeTimeout = 3600000) {
        if (this.cache[name]) {
            return this.cache[name];
        }
        const newCache = new class {
            constructor() {
                this.size = 0;
                this.data = {}
                this.allowTicks = true;
                this.handlerAttached = false;
                this.alive = false;
                this.tick = this.tick.bind(this);
                this.tickPointer = null;
                this.debug = true;
            }

            write(key, data) {
                const item = {
                    created: Date.now(),
                }
                if (typeof data == 'object') {
                    item.data = {...data};
                } else {
                    item.data = data;
                }

                this.data[key] = {...item};
                this.size = Object.keys(this.data).length;
                return item;
            }

            read(key) {
                return this.data[key]?.data;
            }

            async tick() {
                if (!this.alive) return;
                if (this.busy) return;
                this.busy = true;
                this.broker.debug(3, `[Event] Cache::${this.name} tick`)
                Object.keys(this.data).map((key) => {
                    const now = Date.now();
                    if (now - this.data[key].created > this.purgeTimeout) {
                        this.broker.debug(3, `[Event] Cache::${this.name}->${key} deleted (${this.data[key]})`);
                        delete this.data[key];
                        this.size = Object.keys(this.data).length;
                    }
                })
                this.busy = false;
            }

            start() {
                this.alive = true;
                this.tickPointer = setInterval(this.tick, this.purgeTimeout);
            }

            stop() {
                this.alive = false;
                clearInterval(this.tickPointer);
            }
        }
        newCache.broker = this;
        newCache.purgeTimeout = purgeTimeout;
        newCache.name = name;
        this.cache[name] = newCache;
        this.caches.push(newCache);
        if (purgeTimeout != 0) {
            newCache.start();
        }
        return newCache;
    }

    sync(dumpFile = this.config.dumpFile) {
        this.debug(1, `[Mesg] Shutting down`);
        try {
            fs.writeFileSync(dumpFile, JSON.stringify({
                queues: Object.keys(Broker.queue).reduce((result, el) => {
                    result[el] = {messages: Broker.queue[el].messages}
                    return result;
                }, {}),
                caches: Object.keys(Broker.cache).reduce((result, el) => {
                    result[el] = {data: Broker.cache[el].data}
                    return result;
                }, {}),
            }));
            this.debug(2, `[Event] state dump ${this.config.dumpFile} saved`);

        } catch (e) {
            this.debug(1, `[Warn] state dump ${this.config.dumpFile} not saved`);
        }
    }
}

const Broker = new BrokerClass();

module.exports = Broker