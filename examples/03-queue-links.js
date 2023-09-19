const broker = require('../broker');

broker.configureHTTP({ port: 8080 });
//broker.clear(); // forget preious restored state (or not)
const webHookQueue = broker.createQueue("input");
console.log("queue created",broker.queues);

const messageProcessingBeforePushToQueue = (inputData, queue) => {
    return {
        url: inputData.url,
        query: inputData.query,
        body: inputData.body,
    }; // to pass into queue
}
const createResponse = (inputData, queue) => {
    return {
        queueInfo: queue.length,
        data: queue.messages[0].data.query.a,
        currentSumA: queue.messages.reduce((n, el) => n + Number(el.data.query.a), 0),
        currentAvgA: queue.messages.reduce((n, el) => n + Number(el.data.query.a), 0) / queue.length
    }
}

webHookQueue.attachInputWeb('/api/test', messageProcessingBeforePushToQueue, createResponse, 5000)
const writer = broker.createQueue("writer");
console.log("queue created",broker.queues);

writer
    .attachInput(webHookQueue)
    .setHandler(async (msg, queue) => {
        console.log(`Q: ${queue.name} > Internal message on Webserver or any other logic for message ${msg}`)
    }, 3000);

console.log("queue attached", writer);

broker.startHttp();
