const express = require("express");
const app = express();
const AWS = require('aws-sdk');
const PORT = process.env.PORT || 5672;

app.use(express.json());

const amqp = require("amqplib");
var channel, connection;

const exchange_name = "[name]";
const exchange_type = "[type]";
const queue_name = "[queue]";

connectToRabbitMQ();
async function connectToRabbitMQ() {
  try {
    connection = await amqp.connect("amqp://[url:port]");
    channel = await connection.createChannel();

    connectToQueue();
  } catch (error) {
    console.log(error);
  }
}

async function connectToQueue() {
  // https://amqp-node.github.io/amqplib/channel_api.html#channel_assertExchange
  await channel.assertExchange(exchange_name, exchange_type, {
    durable: false,
  });

  const s3 = new AWS.S3();
  const bucketName = '[bucket name]';
  const fileName = '[file name]'; // name up to you

  const q = await channel.assertQueue(queue_name, { exclusive: true });

  // binding the queue
  const binding_key = "";
  channel.bindQueue(q.queue, exchange_name, binding_key);

  console.log("Consuming messages from queue: ", q.queue);
  channel.consume(
    q.queue,
    (msg) => {
      if (msg.content)
        console.log("\nReceived message: ", msg.content.toString());
        channel.ack(msg);

        const params = {
            Bucket: bucketName,
            Key: fileName,
            Body: msg.content.toString()
        };
    
        s3.upload(params, function(s3Err, data) {
            if (s3Err) throw s3Err
            console.log(`File uploaded successfully at ${data.Location}`)
        });
    }
  );

}

app.listen(PORT, () => console.log("Server running at port " + PORT));