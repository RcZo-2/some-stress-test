package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.Random;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.kafka.common.protocol.types.Type.UUID;

public class Main {

    private static final String BOOTSTRAP_SERVERS = "idontknow";

    public static void main(String[] args) {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"idontknow\" password=\"idontknow\";");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "JsonProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);
        Boolean isRoundOver = false;
        int oneRoundBatch = 1;
        int roundCounter = 0;
        int tps = 3;
        // Random target topic
        Random random = new Random();
        //String[] randomTopic = {"CDIP-CUB-DAT-R-LOGIN", "CDIP-CXL-DAT-R-LOGIN", "CDIP-SEC-DAT-R-LOGIN"};
        String[] randomTopic = {"TEST97639-INPUT"};
        String chosenTopic;

        // Produce a random JSON message with a key and send to Kafka topic
        try {
            while (true) {

                chosenTopic = randomTopic[random.nextInt(randomTopic.length)];
                //System.out.print(chosenTopic);
                //String key = generateKey(); // Generate a random key
                String key = getRandomCustomerId();
                JSONObject message = generateRandomMessage();
                ProducerRecord<String, String> record = new ProducerRecord<>(chosenTopic, key, message.toString());

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Failed to send message: " + exception.getMessage());
                        } else {
                            System.out.println(" : " + key + " : " + message.toString());
                        }
                    }
                });
                roundCounter++;
                if (roundCounter == oneRoundBatch) {
                    roundCounter = 0;
                    Thread.sleep(1000 / tps);
                } else {
                    continue;
                }
                // Adjust delay as needed
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static JSONObject generateRandomMessage() {
        Random random = new Random();

        // Generate random values for the message

        int messageId = random.nextInt(10);
        Double longitude = getRandomLongitude();
        Double latitude = getRandomLatitude();

        // Create JSON object for the message
        JSONObject jsonMessage = new JSONObject();

        jsonMessage.put("LoginSerialNumber", java.util.UUID.randomUUID().toString());
        //jsonMessage.put("customer_id", getRandomCustomerId());
        jsonMessage.put("login_date", getCurrentDateAsString());
        jsonMessage.put("login_time", getCurrentTimeAsString());
        jsonMessage.put("login_status", getRandomLoginStatus());
        jsonMessage.put("login_channel", "ch");
        jsonMessage.put("login_country_code", getRandomLoginCountryCode());

        String randomId = getRandomDeviceId();
        if (Math.random() < 0.5) {
            jsonMessage.put("login_type", "APP");
            jsonMessage.put("app_device_id", randomId);
            jsonMessage.put("web_device_id", JSONObject.NULL);
            jsonMessage.put("model", getRandomModel());
            jsonMessage.put("app_os_type", getRandomAppOS());
            jsonMessage.put("app_os_type_version", getRandomAppOSVer());
            jsonMessage.put("web_os_type", JSONObject.NULL);
            //jsonMessage.put("web_os_type_version", JSONObject.NULL);
            jsonMessage.put("browser", JSONObject.NULL);
        } else {
            jsonMessage.put("login_type", "WEB");
            jsonMessage.put("app_device_id", JSONObject.NULL);
            jsonMessage.put("web_device_id", randomId);
            jsonMessage.put("model", JSONObject.NULL);
            jsonMessage.put("app_os_type", JSONObject.NULL);
            jsonMessage.put("app_os_type_version", JSONObject.NULL);
            jsonMessage.put("web_os_type", getRandomWebOS());
            //jsonMessage.put("web_os_type_version", getRandomWebOSVer());
            jsonMessage.put("browser", getRandomBrowser());
        }
        jsonMessage.put("ip", getRandomIp());
        //jsonMessage.put("maintain_tag", "mtag");
        return jsonMessage;
    }

    private static String generateKey() {
        Random random = new Random();
        return String.valueOf(random.nextInt(1000));
    }

    private static String getRandomCustomerId() {
        Random random = new Random();
        //return "A1234" + String.format("%05d", random.nextInt(99999) + 1);
        return "@12000000" + String.format("%01d", random.nextInt(1) + 1);
        //return "@120" + String.format("%06d", random.nextInt(999999) + 1);
    }

    public static String getCurrentDateAsString() {
        ZonedDateTime utcNow = ZonedDateTime.now(ZoneId.of("UTC"));
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
        return utcNow.format(formatter);
    }


    public static String getCurrentTimeAsString() {
        ZonedDateTime utcNow = ZonedDateTime.now(ZoneId.of("UTC"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        return utcNow.format(formatter);
    }

    public static String getRandomLoginStatus() {
        Random random = new Random();
        String[] choices = {"Y", "Y", "Y", "N"};
        //String[] choices = {"YF"};
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    public static String getRandomIp() {
        Random random = new Random();
        int octet1 = 180;
        int octet2 = 217;
        int octet3 = random.nextInt(256);
        int octet4 = random.nextInt(256);
        return octet1 + "." + octet2 + "." + octet3 + "." + octet4;
    }

    public static String getRandomLoginCountryCode() {
        Random random = new Random();
        String[] choices = {"TW", "TW", "TW", "TW", "EN", "CN", "US", "JP", "KO", "AU", "SG", "RU", "FR", "CA", "IT", "VN", "DE"};
        //String[] choices = {"TW", "GGG"};
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    public static String getRandomDeviceId() {
        Random random = new Random();
        return String.format("%06d", random.nextInt(9999) + 1);
    }


    public static String getRandomModel() {
        Random random = new Random();
        //String[] choices = {"Samsung", "Apple", "Nokia", "Xiaomi", "Oppo", "Sony", "Huawei", "Vivo", "LG"};
        String[] choices = {"Samsung"};
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    public static String getRandomAppOS() {
        Random random = new Random();
        String[] choices = {"Andoird", "iOS"};
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    public static String getRandomWebOS() {
        Random random = new Random();
        String[] choices = {"Win10", "MacOS"}; //"Andoird", "Ubuntu",
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    public static String getRandomAppOSVer() {
        Random random = new Random();
        String[] choices = {"v1.0.0"}; //, "v2.0.0", "v3.0.0", "v4.0.0"
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    public static String getRandomWebOSVer() {
        Random random = new Random();
        String[] choices = {"v1.0.0"}; //, "v2.0.0", "v3.0.0", "v4.0.0"
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    public static String getRandomBrowser() {
        Random random = new Random();
        String[] choices = {"Firefox"}; //"IE", "Edge", "Chrome",
        int index = random.nextInt(choices.length);
        return choices[index];
    }

    private static Double getRandomLongitude() {
        Random random = new Random();
        return 0.0;
        //return random.nextDouble()* 360 - 180;
    }

    private static Double getRandomLatitude() {
        Random random = new Random();
        return random.nextDouble() * 6 - 3;
    }
}