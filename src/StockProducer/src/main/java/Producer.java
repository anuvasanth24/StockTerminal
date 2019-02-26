import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.io.File;
import java.util.Random;


/**
 * Producer class for the purposes of ingesting data by the Kafka servers
 */

public class Producer {



    private HashMap<String, Double> ConstructStockBasePrice(){

        String stockPriceFileName = Constants.FILENAME_STOCK_PRICE;
        File stockFile = new File(stockPriceFileName);

        HashMap<String, Double> hmap = new HashMap();

        // stockFile line example = FB,120
        // Parsing stock file and storing symbol and its price as <key,value> pair in a hashmap to be used as a base price

        try
        {
            Scanner stockPriceScanner = new Scanner(stockFile);

            while (stockPriceScanner.hasNextLine()) {
                String line = stockPriceScanner.nextLine();
                String[] lineArr = line.split(",");
                hmap.put('"'+lineArr[0]+'"',Double.parseDouble(lineArr[1]));

            }
        }
        catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        }

        return hmap;
    }

    /**
     * Function mocks stock data by adding a random Integer value to a base price that is retrieved from hmap for each stock
     * Each stock data contains symbol, price, timestamp, market cap, sector amd Industry as its fields
     *
     * Constants.KAFKA_SERVER public DNS and port of the servers retreived from Constants.java
     */
    public void Send () throws java.lang.InterruptedException {

        String fileName = Constants.FILENAME_COMPANY_LIST;

        File file = new File(fileName);

        Random rn = new Random();

        //constructing stock base price hashmap
        HashMap<String, Double>  baseHMap = ConstructStockBasePrice();

        //Kafka Congifuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.KAFKA_KEY_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.KAFKA_VALUE_SERIALIZER);
        KafkaProducer<String, String> kProducer = new KafkaProducer<String, String>(props);

        try {

            Scanner inputStream = new Scanner(file);


            //Simulating additional stock data such as current time and price to make it fit for streaming data

            while (inputStream.hasNextLine()) {
                String line = inputStream.nextLine();

                String[] splitLine = line.split(",");
                double priceAdd = rn.nextInt(50);

                //simulating price of each stock based on actual stock price from the above hashmap
                String price = String.valueOf(baseHMap.get(splitLine[0]) + priceAdd);
                //adding price and current time for each stock symbol
                line = line + "," + '"' + price + '"' + "," + '"' + LocalDateTime.now().toString() + '"';
                ProducerRecord<String, String> data;

                data = new ProducerRecord<String, String>("topic-stock", line);
                kProducer.send(data);
                System.out.println(data);
                kProducer.flush();
            }

            inputStream.close();
        }
        catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        }

        kProducer.close();
        System.out.println("Producer flushed and closed");
    }

    public static void main(String[] args){

        Producer producer = new Producer();
        try {
            producer.Send();
        }
        catch(InterruptedException ie){
            System.out.println(ie.getMessage());
        }

    }
}

