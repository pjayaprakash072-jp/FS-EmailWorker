const {Kafka} = require('kafkajs');

const ca_cert = require('./certs/ca.pem')
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