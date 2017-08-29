/**
 * Created by rganye on 8/28/17.
 */
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'task_queue';  // queue name
        const msg= process.argv.slice(2).join(' ') || "Hello world from workers!";  //
        ch.assertQueue(q,{durable: true}); // affirm its a queue. Durable is set to true;
        ch.sendToQueue(q, new Buffer.from(msg), {persistent : true}); // message-task pushed to the queue
        console.log(" [x] Sent '%s' ", msg)
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});
