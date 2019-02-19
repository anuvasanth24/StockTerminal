public final class Constants {

    private Constants(){}

    public static String FILENAME_COMPANY_LIST = "/Users/anupama/Documents/GitHub/StockAnalyzer/Producer/src/main/java/clist.csv";
    public static String FILENAME_STOCK_PRICE =  "/Users/anupama/Documents/GitHub/StockAnalyzer/Producer/src/main/java/stocksymbols.csv";

    public static String KAFKA_SERVER = "ec2-52-13-197-44.us-west-2.compute.amazonaws.com:9092";
    public static String KAFKA_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String KAFKA_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static String KAFKA_TOPIC = "topic_stock";
}

