const broker = require('../broker');

broker.configureHTTP({ port: 8080 });
const webHookQueue = broker.createQueue("input");
console.log("queue created",broker.queues);

const numberCache = broker.createCache('numbers', 5000);

const createResponse = (inputData, queue) => {
    console.log(`reading cache with ${inputData.query.a} key:`, numberCache.read(inputData.query.a) || 0)
    const count = numberCache.read(inputData.query.a)?.count || 0;
    console.log(`count is:`, count)
    console.log(`cache is:`, numberCache.data)
    numberCache.write(inputData.query.a, { count: count + 1 })
    return { key: inputData.query.a, count: count + 1 }
}

webHookQueue.attachInputWeb('/api/test', null, createResponse, 5000)

broker.startHttp();

//you could play with different /api/test/?a=N