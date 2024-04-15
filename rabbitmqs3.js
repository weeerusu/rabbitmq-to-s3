const amqp = require('amqplib/callback_api');
const AWS = require('aws-sdk');

exports.handler = async (event) => {
    const s3 = new AWS.S3();
    const bucketName = '<Your_Bucket_Name>';
    const fileName = '<Your_File_Name>';

    amqp.connect('amqp://<RabbitMQ_Host>', function(error0, connection) {
        if (error0) {
            throw error0;
        }
        connection.createChannel(function(error1, channel) {
            if (error1) {
                throw error1;
            }

            const queue = '<Your_Queue_Name>';

            channel.consume(queue, function(msg) {
                console.log(" [x] Received %s", msg.content.toString());

                const params = {
                    Bucket: bucketName,
                    Key: fileName,
                    Body: msg.content.toString()
                };

                s3.upload(params, function(s3Err, data) {
                    if (s3Err) throw s3Err
                    console.log(`File uploaded successfully at ${data.Location}`)
                });

            }, {
                noAck: true
            });
        });
    });
};
