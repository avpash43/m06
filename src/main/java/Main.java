import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.CageGeocoderUtil;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net","OAuth");
        conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", "f3905ff9-16d4-43ac-9011-842b661d556d");
        conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT");
        conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token");
        conf.set("fs.azure.account.key.stsparkbasicwesteurope.dfs.core.windows.net", "arh+EEs7+t+Pw1G22RzfuQGCYbKQpGz09vUUdT2XGJ/VQ3i/TkpTyFuXL/NF9ZbZA62Zvo3jXV9uOAM8amJnPg==");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkBasic")
                .config(conf)
                .getOrCreate();

        Dataset<Row> datasetHotels = spark.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels");

        Dataset<Row> datasetWeather = spark.read()
                .parquet("abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather");

        DataHandler dataHandler = new DataHandler(new CageGeocoderUtil());

        Dataset<Row> filteredHotels = dataHandler.filterHotelsLatLng(datasetHotels);
        Dataset<Row> checkedHotels = dataHandler.checkHotelsLatLng(filteredHotels);
        Dataset<Row> checkedHotelsWithGeoHash = dataHandler.generateGeoHash(checkedHotels, "Geohash", "Latitude", "Longitude");
        Dataset<Row> datasetWeatherWithGeoHash = dataHandler.generateGeoHash(datasetWeather, "Geohash", "lat", "lng");

        Dataset<Row> joinedDatasets = dataHandler.joinDatasets(datasetWeatherWithGeoHash, checkedHotelsWithGeoHash, "Geohash", "Name", "Country", "City", "Address", "wthr_date", "year", "month", "day");

        dataHandler.writeDatasetAsParquet(joinedDatasets, "abfss://data@stsparkbasicwesteurope.dfs.core.windows.net/data/result.parquet", "year","month", "day");

        spark.stop();
    }
}
