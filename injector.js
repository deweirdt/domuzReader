require('dotenv').config()
const amqp = require('amqp-connection-manager');
const xml2js = require('xml2js');

const http = require('http');
const { read } = require('fs');
parser = new xml2js.Parser( {
    normalizeTags: true,
    normalize: true,
});

amqpConnection = 'amqp://' + process.env.AMQP_USERNAME +':'+process.env.AMQP_PASSWORD +'@'+process.env.AMQP_HOST;

const connection = amqp.connect([amqpConnection]);

connection.on('connect', () => console.log('AMQP Connected!'));
connection.on('disconnect', err => console.log('AMQP Disconnected.', err.stack));

// Create a channel wrapper
const channelWrapper = connection.createChannel({
  json: true,
  setup: channel => channel.assertExchange(process.env.AMQP_EXCHANGE, 'fanout')
});

const options = {
    //...
    hostname: process.env.DOMUZ,
    path: '/data/static.xml',
    timeout: 3000,
};  

console.log("Domuz connection are: %j", options);

function readDomuzdata() {
    var req = http.get(options, (resp) => {
        var packet = '';
        
        resp.on('data', (data) => {
            packet += data;
        });
    
        resp.on("end", () => {
            try {
                parser.parseString(packet, function(err, result) {
                    if(err) {
                        console.log("Problem in parsing: ", err);

                    } else {
                        domuzData = parseData(result);
                        console.log("Result is: %s", domuzData.date);
                        publishDomuzData(domuzData);
                        
                    }
                    packet = '';
                });
            } catch(err) {
                console.log("Issue with parsing data: ", err);
                packet = '';
            }
        });

        resp.on("error", (err) => {
            console.log("Error received: ", err);
        });
    
    }).end();
    req.on('error', function(e) {
        console.log("Req error: ", e);
        req.abort();
    });
    req.on('timeout', function(e) {
        console.log("Req timeout: ", e);
        req.abort();
    });
    req.on('uncaughtException', function(e) {
        console.log("Req uncaughtException: ", e);
        req.abort();
    });
}

function parseData(data) {

    let heatArea = [];
    let heatCtrls = [];
    try {
        for(let heatCtrl of data.devices.device[0].heatctrl) {
            let dummy = {
                inuse: !!Number(heatCtrl.inuse[0]),
                heatCtrlNumber: Number(heatCtrl.$.nr),
                heatAreaNr: Number(heatCtrl.heatarea_nr[0]),
                state: Number(heatCtrl.heatctrl_state[0]),
                valveState: Number(heatCtrl.actor_percent[0])
            }
            heatCtrls.push(dummy);
        }
        
        for(let area of data.devices.device[0].heatarea) {
            let dummy = {
                name: area.heatarea_name[0],
                areaNumber: Number(area.$.nr),
                actualTemp: Number(area.t_actual[0]),
                targetTemp: Number(area.t_target[0]),
                requestedDayTemp: Number(area.t_heat_day[0]),
                requestedNightTemp: Number(area.t_heat_night[0]),
                areaState: area.heatarea_state[0],
                mode: getHeaterMode(area.heatarea_mode[0]),
                heatCtrls: heatCtrls.filter(x => x.heatAreaNr === Number(area.$.nr))
            }
            heatArea.push(dummy);
        }

        let domuzData = {
            date: new Date(),
            name: data.devices.device[0].name[0],
            pump: {
                active: !!Number(data.devices.device[0].pump_output[0].pump_isactive[0])
            },
            heatArea: heatArea,
            heatCtrls: heatCtrls
        }
        //console.log("HeatArea: %j", domuzData);
        return domuzData;
    } catch(err) {
        console.log("Could not parse the data: %j", data);
    }
}

function getHeaterMode(data) {
    let mode;
    switch(data) {
        case '0': mode = "auto";
            break;
        case '1': mode = "day";
            break;
        case '2': mode = "night";
            break;
        case '3': mode = "off";
            break;
        default: mode = "unknown";
            break;
    }
    return mode;
}

function publishDomuzData(domuzData) {
    console.log("Publishing %j", domuzData);
    channelWrapper.publish(process.env.AMQP_EXCHANGE, '', JSON.parse(JSON.stringify(domuzData)), { contentType: 'application/json', persistent: true })
        .then(function() {
            console.log("Message pushed on the AMPQ");
        })
        .catch(err => {
            console.log("Message was rejected:", err.stack);
            channelWrapper.close();
            connection.close();
        });
}

//readDomuzdata();
//Read each minute
//setInterval(readDomuzdata, 5000);
setInterval(readDomuzdata, 60000);
