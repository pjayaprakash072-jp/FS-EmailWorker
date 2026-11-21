const {Kafka} = require('kafkajs');
const fs = require('fs');
const path = require('path');

const ca_cert = fs.readFileSync(
  path.join(__dirname,"certs","ca.pem"),
  "utf-8"
);// load file completely (sync), normal readFile is async.
const kafka = new Kafka(
  {
    clientId: process.env.KAFKA_CLIENT_ID,
    brokers:(process.env.KAFKA_BROKERS || 'localhost:9092').split(","),

    ssl:{
      ca:[ca_cert]
    },
    sasl:{
      mechanism: process.env.KAFKA_SASL_MECHANISM || 'plain',
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD
    },

    retry:{
      initialRetryTime:300,
      retries:5
    },
    connectionTimeout:10000,
    requestTimeout: 30000
  }
)

module.exports = kafka;