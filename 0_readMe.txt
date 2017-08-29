Begin by installing rabbit mq

on mac ox,
```
brew install rabbitmq

```
then start the rabbitmq server
``` brew services start rabbitmq ```

on pc,
```
npm install amqplib

```
--------------some commandds for rabbitMQ --------
- list out all exchanges currently running:
sudo rabbitmqctl list_exchanges

- list out all bindings : exchange-queues
rabbitmqctl list_bindings