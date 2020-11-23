var amqp = require('amqp-connection-manager');
require('dotenv').config();
const Influx = require('influx');
const nano = require('nano-seconds');

var mongo = require('mongodb').MongoClient;
var url = 'mongodb://' + process.env.MONGO_DB + ':27017/';
var dbo;

const mongoConnection = mongo.connect(url, function(err, db) {
    if (err) throw err;
    dbo = db.db("domuz");
});


amqpConnection = 'amqp://' + process.env.AMQP_USERNAME +':'+process.env.AMQP_PASSWORD +'@'+process.env.AMQP_HOST;
const connection = amqp.connect([amqpConnection]);
connection.on('connect', () => console.log('AMQP Connected!'));
connection.on('disconnect', err => console.log('AMQP Disconnected.', err.stack));

// Set up a channel listening for messages in the queue.
var channelWrapper = connection.createChannel({
    setup: channel => {
        // `channel` here is a regular amqplib `ConfirmChannel`.
        return Promise.all([
            channel.assertQueue(process.env.AMQP_QUEUE, { durable: true }),
            channel.assertExchange(process.env.AMQP_EXCHANGE, 'fanout'),
            channel.prefetch(1),
            channel.bindQueue(process.env.AMQP_QUEUE, process.env.AMQP_EXCHANGE, ''),
            channel.consume(process.env.AMQP_QUEUE, onMessage)
        ])
    }
});

channelWrapper.waitForConnect()
.then(function() {
    console.log("Listening for AMQP messages");
});

const influx = new Influx.InfluxDB({
    host: process.env.INFLUX_HOST,
    database: process.env.INFLUX_DB_NAME,
});

influx.getDatabaseNames()
    .then(names => {
        if (!names.includes(process.env.INFLUX_DB_NAME)) {
            return influx.createDatabase(process.env.INFLUX_DB_NAME);
          }
    })
    .then(() => {
    });

const onMessage = data => {
    var message = JSON.parse(data.content.toString());
    console.log("Received message: %j", message);
    //storePumpState(message);
    var promiseList = [];
    promiseList.push(storeRawMessage(message));

    for(let room of message.heatArea) {
        promiseList.push(storeRoomTemp(message, room));
    }
    
    for(let heatCtrl of message.heatCtrls) {
        promiseList.push(storeHeatCtrls(message, heatCtrl));
    }
    
    //console.log("List of promisses: ", promiseList);
    Promise.all(promiseList).then((values) => {
        console.log("All done");
        //channelWrapper.ack(data);
    });
}

function storeRawMessage(message) {
    return new Promise((stored, reject) => {
        dbo.collection("rawdata").insertOne(message, function(err, res) {
            if (err) {
                reject();
                throw err;
            } else {
                stored();
            }
            console.log("1 document inserted");
          });
    });
    
}

function storeHeatCtrls(domuz, heatCtrl) {
    return new Promise((stored, reject) => {
        influx.writePoints([
            {
                measurement: 'heatCtrls',
                tags: {
                    collector: domuz.name,
                    heatCtrlNumber: heatCtrl.heatCtrlNumber
                },
                fields: {  
                    state: heatCtrl.state,
                    percentage: heatCtrl.valveState
                },
                timestamp: nano.toString(nano.fromISOString(domuz.date))
            }
        ]).then(() => {
            console.log("Stored: storeHeatCtrls %s Ctrlnumber: %s", domuz.name, heatCtrl.heatCtrlNumber);
            stored();
        }).catch((err) => {
            console.log("Error while writingPoints: ", err.message);
            reject();
        });  
    });
}

function storeRoomTemp(domuz, room) {
    return new Promise((stored, reject) => {
        influx.writePoints([
            {
                measurement: 'roomTemp',
                tags: {
                    collector: domuz.name,
                    room: room.name,
                },
                fields: {  
                    pump: domuz.pump.active ? 1 : 0,
                    actualTemp: room.actualTemp,
                    targetTemp: room.targetTemp,
                    requestedDayTemp: room.requestedDayTemp,
                    requestedNightTemp: room.requestedNightTemp,
                    areaState: room.areaState,
                    mode: room.mode
                },
                timestamp: nano.toString(nano.fromISOString(domuz.date))
            }
        ]).then(() => {
            console.log("Stored: ", room.name);
            stored();
        }).catch((err) => {
            console.log("Error while writingPoints: ", err.message);
            reject();
        });  
    });
}