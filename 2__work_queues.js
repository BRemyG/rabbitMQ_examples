/**
 * Created by rganye on 8/28/17.
 */

// Here, we will create a WorkQueue (aka: Task queues) that will be used to distribute time consuming tasks among multiple workers.
// the reason for this is to avoid doing resource intensive tasks immediately and waiting for it to complete.
// Instead, we schedule tasks for the receiver to be done later. We encapsulate tasks and eventually execute the job.
// When you run many workers, the tasks are shared between them.
const amqp = require('amqplib/callback_api'); // require rabbit MQ

// 1) P->Q send new task to work queue
// run 2_P->Q.js ...... (argv is 6 dots. receiver will wait 6 seconds)

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

// 2) Q->R modify the Receive task to worker queue
// run 2_Q->R.js

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'task_queue'; // queue name to connect with.
        ch.assertQueue(q, {durable: true}); // affirm its a queue. Durable set to true
        ch.consume(q, function(msg){
            // perform work on received message
            var secs = msg.content.toString().split('.').length-1;
            console.log(" [x]Received %s", msg.content.toString());
            setTimeout(function(){
                console.log("[x] Done '%s' seconds", secs)
            }, secs * 1000)
        }), {noAck: true } // do not acknowledge receiving this message. (we'll deal with this later)
    })
});

// 3) Q->R with no-acknowledgement set to false. dead-worker-fail-safe
// we want some form of notification when worker completes the message.
// with this setting, if the worker terminates without completing task, that task will NOT
// be lost, but transferred to another running worker.
// mark noAck as false.
amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'task_queue'; // queue name to connect with.
        ch.assertQueue(q, {durable: true}); // affirm its a queue. Durable set to true
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



// 4) P->Q . crashed-server-fail-safe.
// when server fails or crashes, it will forget the queue unless we tell it not to.
// 2 things are required to prevent this. In produce code,
// mark queue as durable,
// mark messages as durable

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = 'task_queue';  // queue name
        const msg= process.argv.slice(2).join(' ') || "Hello world from workers!";  //
        ch.assertQueue(q,{durable: true}); // Durable: true, Preserves queue in-case of failed server;
        ch.sendToQueue(q, new Buffer.from(msg), {persistent : true}); // persistent:true, tell rabbit mq to save msg to disk
        console.log(" [x] Sent '%s' ", msg)
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});


// 5) Q->R. receive one job at a time.
// When server is working on a job, it can queue up other jobs assigned to it.
// We can set it to prefetch only one job, so that it doesn't keep a job queue.
// When this happens, an idle worker gets the job.
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

// 6) P->Q generate non durable queue with generated name

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const q = '';  // '' will be automatically given a random queue name (e.g. amq.gen-JzTY20BRgKO-HjmUJj0wLg )
        const msg= process.argv.slice(2).join(' ') || "Hello world from workers!";  //
        ch.assertQueue(q,{exclusive: true}); // Exclusive: true, queue gets deleted after connection is closed ;
        ch.sendToQueue(q, new Buffer.from(msg), {persistent : true}); // persistent:true, tell rabbit mq to save msg to disk
        console.log(" [x] Sent '%s' ", msg)
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});