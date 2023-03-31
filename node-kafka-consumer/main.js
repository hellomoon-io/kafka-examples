const {Kafka} = require('kafkajs');

const bootstrapServers = "bootstrap servers"; // Contact us for access.
const username = "username";
const password = "password";
const topic = "topic";

async function main() {
    const consumer = new Kafka({
        brokers: bootstrapServers.split(","),
        sasl: {
            mechanism: "SCRAM-SHA-512",
            username: username,
            password: password
        },
        ssl: true,
    }).consumer( {
        // Group id doesn't matter as long as it hasn't been used before.
        // Since autoCommit is set to false later, the consumed offset won't be committed to Kafka.
        groupId: `${username}.${Math.random()}`,
    });
    
    await consumer.connect();
    await consumer.subscribe( {
        topic: topic,
        // Start from the latest offset, ignoring all the old events.
        fromBeginning: false,
    });

    await consumer.run({
        autoCommit: false,
        eachMessage: ({ topic, partition, message }) => {
            const update = JSON.parse(message.value.toString());
            
            console.log(`received update for block ${update.blockId}`);
        }
    });
}

main();