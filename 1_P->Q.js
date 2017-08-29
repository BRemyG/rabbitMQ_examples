/**
 * Created by rganye on 8/28/17.
 */
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'hello';  // queue name
        ch.assertQueue(q,{durable: false}); // affirm its a queue
        ch.sendToQueue(q, new Buffer.from('Hello World!')); // message pushed to the queue
        console.log(" [x] Sent 'Hello World!'")
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});
