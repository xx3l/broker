const broker = require('../broker');

broker.configureHTTP({ port: 8080 });
broker.clear(); // forget preious restored state (or not)
const webHookQueue = broker.createQueue("input");
console.log("queue created",broker.queues);

const messageProcessinBeforePushToQueue = (inputData, queue) => {
    console.log("wehook called", inputData, queue, "end of transmission");
    return {
        url: inputData.url,
        query: inputData.query,
        body: inputData.body,
    }; // to pass into queue
}
const createResponse = (inputData, queue) => {
    return JSON.stringify({
        incomingData: {
            url: inputData.url,
        },
        queueInfo: queue.length,
        queueData: JSON.stringify(queue.messages)
    })
}

webHookQueue.attachInputWeb('/api/test', messageProcessinBeforePushToQueue, createResponse)

broker.startHttp();
