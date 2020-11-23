require('dotenv').config()
const amqp = require('amqp-connection-manager');
const fs = require('fs');

//amqpConnection = 'amqp://' + process.env.AMQP_USERNAME +':'+process.env.AMQP_PASSWORD +'@'+process.env.AMQP_HOST;
amqpConnection = 'amqp://iot:iotiot@localhost';
console.log("Connecting to AMQM: %s", amqpConnection);
const connection = amqp.connect([amqpConnection]);

connection.on('connect', () => console.log('AMQP Connected!'));
connection.on('disconnect', err => console.log('AMQP Disconnected.', err.stack));

// Create a channel wrapper
const channelWrapper = connection.createChannel({
  json: true,
  setup: channel => channel.assertExchange('domuz.fanout', 'fanout')
});

function publishDomuzData(domuzData) {
    //console.log("Publishing %j", domuzData);
    channelWrapper.publish('domuz.fanout', '', JSON.parse(JSON.stringify(domuzData)), { contentType: 'application/json', persistent: true })
        .then(function() {
            console.log("Message pushed on the AMPQ");
            setTimeout(assembleTestData, 10, domuzData);
        })
        .catch(err => {
            console.log("Message was rejected:", err.stack);
            channelWrapper.close();
            connection.close();
            setTimeout(assembleTestData, 5000, domuzData);
        });
}

function assembleTestData(domuz) {

    console.log("Data to be send is: ", domuz.date);
    domuz.date = new Date(new Date(domuz.date).getTime() + 1000);
    publishDomuzData(domuz);
} 


let rawdata = fs.readFileSync('../domuz.json');
let domuzData = JSON.parse(rawdata);

setTimeout(assembleTestData, 1000, domuzData);
