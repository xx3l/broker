const broker = require('../broker');

console.log("Spawn queues", broker.queue)

// broker.clear(); // forget preious restored state (or not)
const myQueue1 = broker.createQueue("input1");
const myQueue2 = broker.createQueue("input2");
console.log("queue created",broker.queues);
console.log("myQueue1 get messageCount by object", myQueue1.length);
console.log("myQueue1 get messageCount by broker", broker.queue['input1'].length);
//
 console.log("myQueue1 pushing some data inside");
 for (let i = 0; i < 5; i++)
     myQueue1.write({ data: Math.floor(Math.random()*10) });
// for (let i = 0; i < 5; i++)
//     broker.queue['input'].write({ data: Math.floor(Math.random()*10) });
//
// console.log("myQueue get message count", myQueue.length);
// console.log(broker.queue['input'])
myQueue1.write({a: 1, b: 1}); // manual push data to queue
myQueue2.write({a: 1, b: 1}); // manual push data to queue
console.log(broker.queue['input1'].messages)
console.log(broker.queue['input2'].messages)

for (let i = 0; i < 2; i++) {
    let data = myQueue1.read();
    console.log("Getting data:", data);
}
for (let i = 0; i < 5; i++)
    broker.queue['input1'].write({ data: Math.floor(Math.random()*10) });

broker.sync(); // save broker state manually
