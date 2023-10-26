const amqp = require('amqplib/callback_api');
require('dotenv').config();

var offlinePubQueue = [];
var amqpConn = null;
var channel = null;

module.exports = {
  setupConnection,
  publish,
  consume
}

function setupConnection() {
  console.log("Startup AMQPConnection");
  amqpConnection = 'amqp://' + process.env.RABBIT_MQ_USERNAME +':'+process.env.RABBIT_MQ_PASSWORD +'@'+process.env.RABBIT_MQ_HOST;
  amqp.connect(amqpConnection, function (err, conn) {
  
    if (err) {
      console.error("[AMQP]", err.message);
      console.log("[AMQP] retrying");
      return setTimeout(setupConnection, 1000);
    }
    conn.on("error", function(err) {
      console.log("[AMQP] error happened: ", err);
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function() {
      console.error("[AMQP] reconnecting");
      return setTimeout(setupConnection, 1000);
    });
    console.log("[AMQP] connected");
    amqpConn = conn;
    whenConnected();
  });
  
}

function whenConnected() {
  startPublisher();
}

function startPublisher() {
  amqpConn.createConfirmChannel(function(err, ch) {
    if(closeOnErr(err)) return;

    ch.on("error", function(err) {
      console.log("[AMQP] channel error", err.message);
    });

    ch.on("close", function() {
      console.log("[AMQP] channel closed")
    });

    ch.prefetch(10);
    ch.assertExchange(process.env.RABBIT_MQ_EXCHANGE, 'fanout', {
      durable: true
    });
    
    channel = ch;
    console.log("Flushing the queued messages");
    while(true) {
      queuedMessage = offlinePubQueue.shift();
      if( !queuedMessage ) {
        console.log("No message in the cached sending queue");
        break;
      }
      publish(queuedMessage);
    }
  });
}

function publish(data) {
  try {
    channel.publish(process.env.RABBIT_MQ_EXCHANGE,
                      '', 
                      Buffer.from(JSON.stringify(data)), 
                      { persistent: true },
                      function(err,ok) {
      if(err) {
        console.error("Could not publish put on local stack", err);
        offlinePubQueue.push(data);
        channel.connection.close();
      } else {
        //console.log("Message correctly stored on the AMQP");
      }
      
    });
    } catch(e) {
      console.log("Exception pushing on broker", e.message);
      offlinePubQueue.push(data);
      console.log("local stack is containing: %d, items", offlinePubQueue.length);
    }
}

function closeOnErr(err) {
  if(!err) return false;
  console.log("[AMQP] error", err);
  amqpConn.close();
  return true;
}

function consume(exchangeName, queueName, consumeMethod) {
  try {
    channel.assertQueue(queueName, {durable: true}, function(err, data) {
      channel.bindQueue(queueName, exchangeName, '');
    });
    channel.on("close", function() {
      console.log("Channel was closed");
      setTimeout(consume, 1000, exchangeName, queueName, consumeMethod);
    });
    channel.on("close", function() {
      console.log("Channel was closed");
      setTimeout(consume, 1000, exchangeName, queueName, consumeMethod);
    });

    console.log("Channel has been configred exchangeName: %s, queueName: %s", exchangeName, queueName);
    channel.consume(queueName, function(msg) {
        //Call the callback function to the user
        consumeMethod(msg.content.toString(), function(processed) {
            try{
                if(processed) {
                    console.log("Message got processed, we can ack");
                    channel.ack(msg);
                } else {
                    console.log("Don't do anything I guess, message should remain where it is");
                }
            } catch(e) {
                console.log("Exception when consuming data");
            }
        })
    }, {noAck: false});
  } 
  catch(e) {
      console.log("Channel is not configured yet");
      setTimeout(consume, 1000, exchangeName, queueName, consumeMethod);
  }
}