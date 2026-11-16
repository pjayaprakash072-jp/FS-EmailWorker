const amqp = require('amqplib');
let connection = null;
let channel = null;
let connectionPromise = null;
const connectRabbitMQ = async()=>{
  if(channel){
    return channel;
  }
  if(connectionPromise){
    return connectionPromise;
  }
  if(!process.env.RABBITMQ_URL){
    throw new Error("RABBIT_URL is missing in environment variables");
  }
  connectionPromise = (async()=>{
    try {
      connection = await amqp.connect(
        process.env.RABBITMQ_URL
      );

      channel = await connection.createConfirmChannel();
      await channel.assertQueue("email_queue",{
        durable:true
      })

      console.log("RabbitMQ connected successfully!");

      connection.on("close",()=>{
        connection = null;
        channel = null;
        connectionPromise = null;
        console.log(`RabbitMQ connection closed!`);
      })

      connection.on("error",(error)=>{
        console.error(`RabbitMQ connection error:`,error)
      })

      channel.on("error",(error)=>{
        console.error("RabbitMQ channel error:",error);
      })

      return channel;

    } catch (error) {
      connection = null;
      channel = null;
      throw error;
    }finally{
      connectionPromise = null;
    }
  })();
  return connectionPromise;
}
module.exports = connectRabbitMQ;