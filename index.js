require('dotenv').config();

const express = require('express');
const app = express();

app.use(express.json());

const {
  createTopics
}= require('./src/worker/kafka/createTopics')
const {
  connectConsumer
}= require('./src/worker/kafka/consumer')
const {
  connectProducer
}= require('./src/worker/kafka/producer')

const KAFKA_TOPICS = require('./src/worker/kafka/topics')

const PORT = process.env.PORT || 5007;

const startServer = async()=>{
  try {
    await createTopics();
    await connectConsumer(Object.values(KAFKA_TOPICS));
    await connectProducer();
    app.listen(PORT,()=>{
      console.log("Server is running at port: ",PORT);
    })
  } catch (error) {
    console.error(' Server start Error : ',error);
    process.exit(1);
  }
}

startServer();