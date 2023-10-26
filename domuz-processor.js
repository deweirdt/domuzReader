const amqp = require('./amqp.controller');
require('dotenv').config();
const Influx = require('influx');
const nano = require('nano-seconds');



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
        //This will connect to the rabbitMQ
        amqp.setupConnection();
        amqp.consume(process.env.RABBIT_MQ_EXCHANGE, process.env.RABBIT_MQ_QUEUE, processMsg);
    });

function processMsg(msg, confirm) {
    var message = JSON.parse(msg);
    console.log("Received message: %j", message);
    //storePumpState(message);
    var promiseList = [];
    //promiseList.push(storeRawMessage(message));

    for(let room of message.heatArea) {
        promiseList.push(storeRoomTemp(message, room));
    }
    
    for(let heatCtrl of message.heatCtrls) {
        promiseList.push(storeHeatCtrls(message, heatCtrl));
    }
    
    //console.log("List of promisses: ", promiseList);
    Promise.all(promiseList).then((values) => {
        console.log("All done");
        confirm(true);
    });
}

function storeHeatCtrls(domuz, heatCtrl) {

    //Fix where the heatCtrl.state = null
    if(heatCtrl.state === null) {
        heatCtrl.state = 0;
    }

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