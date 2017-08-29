/**
 * Created by rganye on 8/28/17.
 */
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'hello'; // queue name to connect with.
        ch.assertQueue(q, {durable: false}); // affirm its a queue
        ch.consume(q, function(msg){
            console.log(" [x]Received %s", msg.content.toString())
        }), {noAck: true } // do not acknowledge receiving this message. (we'll deal with this later)
    })
});