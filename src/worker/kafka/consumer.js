const kafka = require('../../config/kafka');
const {
    sendWelcomeEmail,
    sendForgotPasswordLink,
    sendVerificationEmail
} = require('../../services/emailService');
const consumer = kafka.consumer(
  {
    groupId:process.env.KAFKA_GROUP_ID
  }
);

const connectConsumer = async(topics)=>{
  try {
    await consumer.connect();

    console.log("Kafka consumer connected successfully!");

    for(const topic of topics){
      await consumer.subscribe(
        {
          topic,
          fromBeginning:false // we can make true in real production.
        }
      )
    }

    await consumer.run(
      {
        eachMessage: async({topic,partition,message})=>{

          const key = message.key?.toString();
          const payload = JSON.parse(message.value.toString());

          console.log("Received kafka message:",
            {
              key,
              topic,
              partition,
              message
            }
          )
          const type = payload.event;

          switch(type){
            case "WELCOME_EMAIL":
              await sendWelcomeEmail(payload.data.email,payload.data.name);
              break;
            case "FORGOT_PASSWORD":
              await sendForgotPasswordLink(
                payload.data.email,
                payload.data.name,
                payload.data.resetURL
              );
              break;
            case "VERIFY_EMAIL" :
              await sendVerificationEmail(
                payload.data.email,
                payload.data.name,
                payload.data.verificationURL
              )
              break;
            default:
              throw new Error(`Unknown email type : ${type}`)
          }
          console.log("email send successfully!")
        }
      }
    )
  } catch (error) {
    console.log("Kafka consume Error: ", error);
    throw error;
  }
}

const disconnectConsumer = async()=> consumer.disconnect();

module.exports = {
  consumer,
  connectConsumer, 
  disconnectConsumer
}