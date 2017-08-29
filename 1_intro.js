/**
 * Created by rganye on 8/28/17.
 */
console.log("sample into code. DO NOT Execute");
process.exit(0); //prevent from running this code

//--------- rabbit MQ ----------
//https://www.rabbitmq.com/tutorials/tutorial-one-javascript.html

// Rabbit MQ is a message broker, 'middle man'. It receives messages from a producer/sender
// and posts the message to a consumer/receiver.
// Terminology:
// PRODUCING - sending. (P)
// QUEUE - a fifo buffer which rabbit mq uses to store messages that it receives, before sending. (Q)
// CONSUMING - receiving. (R)

//  P -> Q -> R

    //Below, we create a simple syntax common to 'publishers' and 'receivers'.

// 1)
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){   // Connect to rabbit MQ server,

    conn.createChannel(function(err, ch){   // create a channel, and begin doing things ....

        //TODO code goes here.

    })

});

// 2) P -> Q.
// --run this code found in  1_P->Q.js on one terminal
// to send, we must declare a queue, then publish the message to that queue
// declaring a queue is idempotent- it will only be created if it doesn't exist.

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'hello';  // queue name
        ch.assertQueue(q,{durable: false}); // affirm its a queue
        ch.sendToQueue(q, new Buffer.from('Hello World!')); // message pushed to the queue
        console.log(" [x] Sent 'Hello World!'")
    })
});
 setTimeout(function(){
        conn.close();
        process.exit(0);
 }, 500);

// 3) Q -> R
//-- run this code found in 1_Q->R.js in another terminal
// to receive, we open up a connection and a channel and declare the queue from which we will be receiving.
// same process as declaring a new queue. idempotent, meaning it wont create new if producer already created it.
amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'hello'; // queue name to connect with.
        ch.assertQueue(q, {durable: false}); // affirm its a queue
        ch.consume(q, function(msg){
            console.log(" [x]Received %s", msg.content.toString())
        }), {noAck: true } // do not acknowledge receiving this message. (we'll deal with this later)
    })
})
