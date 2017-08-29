/**
 * Created by rganye on 8/28/17.
 */
// public-subscribe: delivering a message to multiple consumers.
// until this point, we have delivered one message to one consumer.
// example, We want one receiver to do log message to a file,
// and another receiver prints on screen.
//
// -- EXCHANGES (X) --
// a messaging model. receives from producer and sends to queue.
// The producer never sends messages directly to the queue.
// It sends it to an EXCHANGE which then sends to a queue.
// The exchange can assign msg to one or many queues, or discard it.
    // Rules of an exchange are defined by the exchange type.(fanout, direct, topic, headers, consistentHash)
//
// P->X->Q->R
//

// 1) P->X ,
    // rule type: 'fanout' -  sends message to all queues at once
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'logs';  // exchange name
        const msg= process.argv.slice(2).join(' ') || "Hello world from workers!";  //
        ch.assertExchange(ex, 'fanout' ,{durable: false}); // Durable is set to false, fanout to all queues.
        ch.publish(ex, '',new Buffer.from(msg)); // '' means we dont want to publish to e specific queue, but we want to publish it to our log exchange.
        console.log(" [x] Sent '%s' ", msg)
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});

// 2) X->Q->R, Binding a queue to an exchange
// The queue is interested in messages from this exchange.
 // rule: fanout send message to all queues at once.
amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'logs'; // exchange name.
        var q='';  // random que name will be generated.
        ch.assertExchange(ex, 'fanout', {durable: false});  // Durable: false - donot preserve queue in-case of failed server;
                                                            // fanout: to all queues
        ch.assertQueue(q, {exclusive: true}, function(err, q){
            console.log("[*] waiting for messages in %s. To exit, press CTRL + C", q.queue);
            ch.bindQueue(q.queue, ex, '') // binds this queue (with random name above) to this exchange

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

// 3) X->Q->R, Binding a queue to an exchange, with key.
// rule: direct - send message to queues  whose binding key matches routing key of the message.
amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'logs'; // exchange name.
        var q='';  // random que name will be generated.
        ch.assertExchange(ex, 'direct', {durable: false});  // Durable: false - donot preserve queue in-case of failed server;
                                                            // fanout: to all queues
        ch.assertQueue(q, {exclusive: true}, function(err, q){
            console.log("[*] waiting for messages in %s. To exit, press CTRL + C", q.queue);
            ch.bindQueue(q.queue, ex, 'key_name') // binds this queue (with random name above) to this exchange,
                                                  // filtering with this key_name

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

// 4) X->Q->R, Binding a queue to an exchange, with key_selector_name.
// rule: direct - send message to queues  whose binding key matches routing key of the message.
amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'logs'; // exchange name.
        var q='';  // random que name will be generated.
        ch.assertExchange(ex, 'direct', {durable: false});  // Durable: false - donot preserve queue in-case of failed server;
                                                            // fanout: to all queues
        ch.assertQueue(q, {exclusive: true}, function(err, q){
            console.log("[*] waiting for messages in %s. To exit, press CTRL + C", q.queue);
            ch.bindQueue(q.queue, ex, 'key_selector_name') // binds this queue (with random name above) to this exchange,
                                                  // filtering with this key_name

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

// 5) P->X , Publishing a message to an exchange with specific key_selector_name
// rule type: 'direct' -  sends message to all queues at once
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'logs';  // exchange name
        const msg= process.argv.slice(2).join(' ') || "Hello world from workers!";  //
        ch.assertExchange(ex, 'direct' ,{durable: false}); // Durable is set to false, fanout to all queues.
        ch.publish(ex, 'key_selector_name',new Buffer.from(msg)); // publish to queues with 'key_selector_name', only.
        console.log(" [x] Sent '%s' ", msg)
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});

// --- example with variable for key 'severity' --
// 6) P->X , Publishing a message to an exchange for queues with keys: info, warning, error
// rule type: 'direct' -  sends message to all queues at once
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'direct_logs';  // exchange name
        const args = process.argv.slice(3);
        const msg= process.argv.slice(2).join(' ') || "Hello world from workers!";  //
        const severity = (args.length>0) ? args[0] : 'info';
        ch.assertExchange(ex, 'direct' ,{durable: false}); // Durable is set to false, fanout to all queues.
        ch.publish(ex, 'key_selector_name',new Buffer.from(msg)); // publish to queues with 'key_selector_name', only.
        console.log(` [x] Sent ${severity} : ${msg}`);
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});

// 7) X->Q->R, Binding a queue to an exchange, with variable key 'severity'.
// rule: direct - send message to queues  whose binding key matches routing key of the message.
const args = process.argv.slice(2);
if (args.length == 0 ){
    console.log("Usage: receive_logs_direct.js [info] [warning] [error]");
    process.exit(1);
}
amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'direct_logs'; // exchange name.
        var q='';  // random que name will be generated.
        ch.assertExchange(ex, 'direct', {durable: false});  // Durable: false - donot preserve queue in-case of failed server;
                                                            // fanout: to all queues
        ch.assertQueue(q, {exclusive: true}, function(err, q){
            console.log("[*] waiting for messages in %s. To exit, press CTRL + C", q.queue);
            args.forEach( severity => {
                ch.bindQueue(q.queue, ex, severity) // binds this queue (with random name above) to this exchange,
                // filtering with this key_name
            })

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

// --------------
// 8) P->X
// rule type: 'topic'
// selectors are a list of words, dot delimited. e.g school.major.class
// with this, we can choose multiple selectors by using * to substitute one word, or # for many words
// eg school.*.class selects for school.BIO.class and school.CHEM.class queues, but NOT for school.CHEM nor school.PHY.101.online
// school.# selects for school.BIO.101, school.BIO, school.BIO.202.online, and school queues
const amqp = require('amqplib/callback_api'); // require rabbit MQ

amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'topic_logs';  // exchange name
        const args = process.argv.slice(2);
        const key = (args.length > 0) ? args[0] : 'anonymous.info'
        const msg= process.argv.slice(1).join(' ') || "Hello world from workers!";  //
        ch.assertExchange(ex, 'topic' ,{durable: false}); // Durable is set to false, fanout to all queues.
        ch.publish(ex, key,new Buffer.from(msg)); // publish to queues with 'key_selector_name', only.
        console.log(` [x] Sent ${key} : ${msg}`);
    });
    setTimeout(function(){
        conn.close();
        process.exit(0);
    }, 500);
});

// 9) X->Q->R
// rule: topic
// selectors are a list of words, dot delimited. e.g school.major.class
// with this, we can choose multiple selectors by using * to substitute one word, or # for many words
// eg school.*.class selects for school.BIO.class and school.CHEM.class queues, but NOT for school.CHEM nor school.PHY.101.online
// school.# selects for school.BIO.101, school.BIO, school.BIO.202.online, and school queues
const args = process.argv.slice(1);
if (args.length == 0 ){
    console.log("Usage: 3_X-\>Q-\>R.js school.class");
    process.exit(1);
}
amqp.connect('amqp://localhost', function(err, conn){
    conn.createChannel( function(err, ch){
        const ex = 'topic_logs'; // exchange name.
        var q='';  // random que name will be generated.
        ch.assertExchange(ex, 'topic', {durable: false});  // Durable: false - donot preserve queue in-case of failed server;

        ch.assertQueue(q, {exclusive: true}, function(err, q){
            console.log("[*] waiting for messages in %s. To exit, press CTRL + C", q.queue);
            args.forEach( topicKey => {
                ch.bindQueue(q.queue, ex, topicKey) // binds this queue (with random name above) to this exchange,

            });

            // ch.prefetch(1); // handle only one job at a time
            ch.consume(q.queue, function(msg){
                // perform work on received message
                var secs = msg.content.toString().split('.').length-1;
                console.log(" [x]exchange Received %s", msg.feilds.routingKey, msg.content.toString());
                setTimeout(function(){
                    console.log("[x] Done '%s' seconds", secs);
                    ch.ack(msg); //channel acknowledge message sent
                }, secs * 1000)
            }), {noAck: false } // Acknowledge receiving this message.
        });
    });
});
