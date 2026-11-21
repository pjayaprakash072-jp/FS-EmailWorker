const path = require('path');

require('dotenv').config(
  {
    path:path.resolve(
      __dirname,"../../.env"
    )
  }
);

const connectRabbitMQ = require('../config/rabbitmq')
const {
    sendWelcomeEmail,
    sendForgotPasswordLink,
    sendVerificationEmail
} = require('../services/emailService');
const startEmailWorker = async()=>{
  const channel = await connectRabbitMQ();
  await channel.prefetch(1);
  console.log(`Email Worker is waiting for jobs...`);
  
  await channel.consume("email_queue",async(msg)=>{
    if(!msg) return;
    try {
      const job = JSON.parse(msg.content.toString());
      const {type,data} = job;
      console.log(`Processing email job: `,job);
      switch(type){
        case "WELCOME_EMAIL":
          await sendWelcomeEmail(data.email,data.name);
          break;
        case "FORGOT_PASSWORD":
          await sendForgotPasswordLink(
            data.email,
            data.name,
            data.resetURL
          );
          break;
        case "VERIFY_EMAIL" :
          await sendVerificationEmail(
            data.email,
            data.name,
            data.verificationURL
          )
          break;
        default:
          throw new Error(`Unknown email type : ${type}`)
      }
      channel.ack(msg);
      console.log(`Email send successfully!`);
    } catch (error) {
      console.log(`Email job failed: ${error}`);
      channel.nack(msg,false,true);
    }
  })
}

startEmailWorker().catch((error)=>{
  console.log("Workder Start up failed.",error);
  process.exit(1);
})
