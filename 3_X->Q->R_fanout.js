/**
 * Created by rganye on 8/28/17.
 */
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'logs'; // exchange name.
        var q='';  // random que name will be generated.
        ch.assertExchange(ex, 'fanout', {durable: true});  // Durable: false - donot preserve queue in-case of failed server;
                                                            // fanout: to all queues
        ch.assertQueue(q, {exclusive: true}, function(err, q){
            console.log("[*] waiting for messages in %s. To exit, press CTRL + C", q.queue);
            ch.bindQueue(q.queue, ex, '') // binds this queue (with random name) to this exchange

            // ch.prefetch(1); // handle only one job at a time
            ch.consume(q.queue, function(msg){
                // perform work on received message
                var secs = msg.content.toString().split('.').length-1;
                console.log(" [x]exchange Received %s", msg.content.toString());
                setTimeout(function(){
                    console.log("[x] Done '%s' seconds", secs);
                    ch.ack(msg); //channel acknowledge message sent
                }, secs * 1000)
            }), {noAck: false } // Acknowledge receiving this message.
        });
    });
});