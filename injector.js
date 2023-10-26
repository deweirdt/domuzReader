require('dotenv').config()
const amqp = require('./amqp.controller');
const xml2js = require('xml2js');
const domuzDevices = require('./domuz-lookup.json');


const http = require('http');
const { read } = require('fs');
const { start } = require('repl');

parser = new xml2js.Parser( {
    normalizeTags: true,
    normalize: true,
});



function startReadout() {
    console.log("Starting readout");
    domuzDevices.forEach( (domuzDevice) => {
      readoutDevice(domuzDevice);
    });
    console.log("Done reading");
}

function readoutDevice(domuzDevice) {
    console.log("Reading data from: ", domuzDevice.alias);

    const options = {
        //...
        hostname: domuzDevice.ip,
        path: '/data/static.xml',
        timeout: 3000,
    };  

    var req = http.get(options, (resp) => {
        let packet = '';
    
        resp.on('data', (chunk) => {
            //console.log("Received part of the data");
            packet += chunk;
        });
    
        resp.on('end', () => {
            //console.log("Got data for: " + domuzDevice.alias + " data: ", packet);
            try {
                parser.parseString(packet, function(err, result) {
                    if(err) {
                        console.log("Problem in parsing: ", err);

                    } else {
                        domuzData = parseData(result);
                        console.log("Result is received for: ", domuzDevice.alias);

                        for(let i=0;i<500;i++) {
                            amqp.publish(domuzData);
                        }


                        amqp.publish(domuzData);
                    }
                    packet = '';
                });
            } catch(err) {
                console.log("Issue with parsing data: ", err);
                packet = '';
            }
        
        });
      }).on("error", (err) => {
        // No idea why I'm having this expection here, just hide it :)
        //console.log("Error: ", err.message);
      })

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


amqp.setupConnection();

startReadout();
setInterval(startReadout, 5000);
console.log('Starting Domuz readout');

