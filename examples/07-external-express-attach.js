const broker = require('../broker')
broker.setDebugLevel(3).clear();

const express = require('express')
const app = express()

app.get('/', (req, res) => { // custom express handler
  res.send('Hello World!') 
})

app.listen(3000, () => {
  console.log('Custom express app started on :3000')
})

broker.attachExpressApp(app)

const testQueue = broker.createQueue("testQueue")

const messageProcessinBeforePushToQueue = (inputData, queue) => {
    console.log(`wehook called. MsgCount in queue::${queue.name}: ${queue.length}`)
    return {
        query: inputData.query
    } // this is a message that goes in queue
}
const createResponse = (inputData, queue) => {
    return JSON.stringify(queue.messages) // just dump queue to web client
}

testQueue.attachInputWeb('/test', messageProcessinBeforePushToQueue, createResponse)
