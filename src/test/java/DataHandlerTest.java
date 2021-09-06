import model.Address;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import util.CageGeocoderUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class DataHandlerTest {
    private static DataHandler dataHandler;
    private static CageGeocoderUtil cageGeocoderUtil;
    private static SparkSession sc;

    @BeforeAll
    static void init() {
        cageGeocoderUtil = mock(CageGeocoderUtil.class, withSettings().serializable());
        dataHandler = new DataHandler(cageGeocoderUtil);
        sc = SparkSession.builder()
                .master("local[*]")
                .appName("test")
                .getOrCreate();
    }

    @Test
    void filterHotelsLatLng() {
        Dataset<Row> dsHotels = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/rawHotels.csv");

        Dataset<Row> dsFilteredHotels = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/filteredHotels.csv");

        Dataset<Row> result = dataHandler.filterHotelsLatLng(dsHotels);

        assertEquals(dsFilteredHotels.collectAsList(), result.collectAsList());
    }

    @Test
    void checkHotelsLatLng() {
        Dataset<Row> dsFilteredHotels = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/checkLatLng.csv");

        Dataset<Row> expected = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/checkLatLngResult.csv");

        Address address = mock(Address.class, withSettings().serializable());

        when(cageGeocoderUtil.getAddress(any(), anyDouble(), anyDouble())).thenReturn(address);
        when(address.getCity()).thenReturn("Coalville");
        when(address.getCountryCode()).thenReturn("us");
        when(address.getRoad()).thenReturn("W 120 S");
        when(address.getHouseNumber()).thenReturn("500");

        Dataset<Row> result = dataHandler.checkHotelsLatLng(dsFilteredHotels);

        assertEquals(expected.collectAsList(), result.collectAsList());
    }

    @Test
    void generateGeoHash() {
        Dataset<Row> dsFilteredHotels = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/filteredHotels.csv");

        Dataset<Row> expected = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/hotelsWithGeohash.csv");

        Dataset<Row> result = dataHandler.generateGeoHash(dsFilteredHotels, "Geohash", "Latitude", "Longitude");

        assertEquals(expected.collectAsList(), result.collectAsList());
    }

    @Test
    void joinDatasets() {
        Dataset<Row> dsWithGeohash = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/hotelsWithGeohash.csv");

        Dataset<Row> dsHWeather = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/weatherWithGeohash.csv");

        Dataset<Row> expected = sc.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv("src/test/resources/joinedWeatherAndHotels.csv");

        Dataset<Row> result = dataHandler.joinDatasets(dsHWeather, dsWithGeohash, "Geohash", "Name", "Country", "City", "Address", "avg_tmpr_f", "avg_tmpr_c", "wthr_date", "year", "month", "day");

        assertEquals(expected.collectAsList(), result.collectAsList());
    }
}