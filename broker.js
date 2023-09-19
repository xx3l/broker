'use strict'

const express = require('express');
const httpBodyParser = require('body-parser');
const fs = require('fs');

class BrokerClass {
    #httpServer;
    constructor() {
        console.log('Broker started...');
        this.#httpServer = false;
        this.version = require('./package.json').version;
        this.clear();
        let data = { queues: {}, caches: {}}
        try {
            data = JSON.parse(fs.readFileSync('./state/dump.json', { encoding: 'utf8', flag: 'r' }));
        } catch (e) {
            data = { queues: {}, caches: {}}
        }
        for (const queue of Object.keys(data.queues || {})) {
            this.createQueue(queue)
            this.queue[queue].messages = data.queues[queue].messages
            this.queue[queue].broker = this;
            console.log(`Queue "${queue}" loaded:`, data.queues[queue])
        }
        for (const cache of Object.keys(data.caches || {})) {
            this.createCache(cache)
            this.cache[cache].data = data.cache[cache].data
            this.cache[cache].broker = this;
            console.log(`Cache "${queue}" loaded:`, data.queues[queue])
        }
        console.log('State restored');
    }
    clear() {
        this.queue = {};
        this.queues = [];
        this.cache = {};
        this.caches = [];
    }
    configureHTTP(config = { port: 3000 }) {
        if (!this.#httpServer) {
            this.httpServerPort = config?.port || 3000;
            this.#httpServer = express();
            this.#httpServer.set("view engine", config.engine || "pug")
            this.#httpServer.set('views', './webroot')
            this.#httpServer.use(httpBodyParser.urlencoded({ extended: true }));
            this.#httpServer.use(httpBodyParser.json());
            this.#httpServer.use(httpBodyParser.raw());

            this.#httpServer.get('/', (req, res) => {
                res.render('index', { broker: this });
            });
        }
    }
    startHttp(mode = 'dev') {
        const signals = [`SIGINT`, `SIGUSR1`, `SIGUSR2`, `SIGTERM`];
        if (mode != 'dev')
            signals.push('uncaughtException');
        signals.forEach(event => {
            process.on(event, () => {
                Broker.sync()
                process.exit(-1);
            });
        })
        this.start();
        this.#httpServer.listen(this.httpServerPort)
    }
    start() {
        this.queues.forEach((queue) => queue.start());
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
                this.setHandler(() => {}, tickInterval)
                this.broker.#httpServer.all(route, (req, res) => {
                    if (!prePushAction) {
                        prePushAction = (req) => {
                            return {
                                url: req.url,
                                query: req.query,
                                body: req.body,
                            }
                        }
                    }
                    const msg = this.write(prePushAction(req, this));
                    msg.state = 'processed';
                    res.send(responseAction(req, this));
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
                    if (this.debug) console.log(`ticked on ${this.name}`);
                    await this.messages.map(async (message) => {
                        if (message.state != 'processed') {
                            if (this.debug) console.log('processing', message);
                            this.handler(message, this);
                            message.state = 'processed';
                        }
                    })
                }
                const lastMsg = this.read();
                if (this.out && lastMsg) {
                    if (this.debug) console.log("transfering", lastMsg);
                    this.out.write(lastMsg.data);
                }
                this.busy = false;
            }
            start() {
                console.log("q started");
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
                const oldest = this.messages.reduce((a,b) => b.created < a ? b.created: a, now)
                return now - oldest;
            }
        }
        newQueue.broker = this;
        newQueue.name = name;
        this.queue[name] = newQueue;
        this.queues.push(newQueue);
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
                    data: {...data}
                }
                this.data[key] = item;
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
                Object.keys(this.data).map((key) => {
                    const now = Date.now();
                    if (now - this.data[key].created > this.purgeTimeout) {
                        if (this.debug) console.log("cache key deleted", key,  this.data[key])
                        delete this.data[key];
                        this.size = Object.keys(this.data).length;
                    }
                })
                console.log(`cache ${this.name} purged`)
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
        newCache.start();
        this.cache[name] = newCache;
        this.caches.push(newCache);
        return newCache;
    }
    sync(dumpFile = "./state/dump.json") {
        console.log('Shutting down');
        fs.writeFileSync(dumpFile, JSON.stringify({
            queues: Object.keys(Broker.queue).reduce((result, el) => {
                result[el] = { messages: Broker.queue[el].messages }
                return result;
            }, {})
        }));
    }
}
const Broker = new BrokerClass();

module.exports = Broker