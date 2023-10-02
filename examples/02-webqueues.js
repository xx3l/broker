const broker = require('../broker');

broker
    .configureHTTP({ port: 8080 })
    .setDebugLevel(3);
console.log(broker.config)
//broker.clear(); // forget preious restored state (or not)
const webHookQueue = broker.createQueue("input");

console.log("queue created",broker.queues);

const messageProcessinBeforePushToQueue = (inputData, queue) => {
    console.log(`wehook called. MsgCount: ${queue.length}`);
    return {
        url: inputData.url,
        query: inputData.query,
        body: inputData.body,
    }; // to pass into queue
}
const createResponse = (inputData, queue) => {
    return {
        incomingData: {
            url: inputData.url,
        },
        queueInfo: queue.length,
        queueData: JSON.stringify(queue.messages)
    }
}

webHookQueue.attachInputWeb('/api/test', messageProcessinBeforePushToQueue, createResponse)

broker.startHttp();
