const broker = require('../broker');

broker.configureHTTP({ port: 8080 }).setDebugLevel(3);
broker.clear();

const counter = broker.createCache('counter', 0);
counter.write('value', 0)

broker.createQueue('increment', 1)
    .attachInputWeb('/api/inc', () => false, () => {
        counter.write('value', counter.read('value') + 1)
        return {current: counter.read('value')}
    });

broker.createQueue('decrement', 1)
    .attachInputWeb('/api/dec', () => false, () => {
        counter.write('value', counter.read('value') - 1)
        return { current: counter.read('value') }
    });
console.log(broker);
broker.startHttp();