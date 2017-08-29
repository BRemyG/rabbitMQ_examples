/**
 * Created by rganye on 8/28/17.
 */
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'logs';  // exchange name
        const msg= process.argv.slice(2).join(' ') || "Hello world from workers!";  //
        ch.assertExchange(ex, 'fanout' ,{durable: true}); // Durable is set to true, fanout to all queues.
        ch.publish(ex, '',new Buffer.from(msg)); // '' means we dont want to publish to e specific queue, but we want to publish it to our log exchange.
        console.log(" [x] Sent '%s' ", msg)
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});
