use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message,
};
use serde_json::Value;


fn main() {
    let bootstrap_servers = "bootstrap servers"; // Contact us for access.
    let username = "username";
    let password = "password";
    let topic = "topic";

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("sasl.username", username)
        .set("sasl.password", password)
        .set("group.id", username.to_owned() + ".group.id")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "SCRAM-SHA-512")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topics");

    loop {
        for message_result in consumer.iter() {
            let message = message_result.expect("Failed to poll for messages");
            let payload = message.payload().unwrap();
            let json = std::str::from_utf8(payload).unwrap();
            let update: Value = serde_json::from_str(json).expect("Failed to parse JSON payload");
            let block_id = update["blockId"].as_u64().unwrap();
            println!("received update for block {}", block_id);
        }
    }
}
