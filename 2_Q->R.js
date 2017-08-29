/**
 * Created by rganye on 8/28/17.
 */
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'task_queue'; // queue name to connect with.
        ch.assertQueue(q, {durable: true}); // Durable: true, Preserves queue in-case of failed server;
        ch.prefetch(1); // handle only one job at a time
        ch.consume(q, function(msg){
            // perform work on received message
            var secs = msg.content.toString().split('.').length-1;
            console.log(" [x]Received %s", msg.content.toString());
            setTimeout(function(){
                console.log("[x] Done '%s' seconds", secs);
                ch.ack(msg); //channel acknowledge message sent
            }, secs * 1000)
        }), {noAck: false } // Acknowledge receiving this message.
    })
});