import com.byteowls.jopencage.JOpenCageGeocoder;
import model.Address;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import util.CageGeocoderUtil;
import util.UdfUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class DataHandler implements Serializable {
    private static final String GEOCODER_API_KEY = "a421f52e792745ef8beb61c4b02df763";
    private final CageGeocoderUtil cageGeocoderUtil;

    public DataHandler(CageGeocoderUtil cageGeocoderUtil) {
        this.cageGeocoderUtil = cageGeocoderUtil;
    }

    public Dataset<Row> filterHotelsLatLng(Dataset<Row> dataset) {
        return dataset
                .where(col("Latitude").isNotNull().and(col("Longitude").isNotNull()))
                .where(col("Latitude").rlike("^-?[0-9][0-9,\\.]+$"))
                .where(col("Longitude").rlike("^-?[0-9][0-9,\\.]+$"));
    }

    public Dataset<Row> checkHotelsLatLng(Dataset<Row> dataset) {
        StructType schema = dataset.schema();
        return dataset.mapPartitions(
                (MapPartitionsFunction<Row, Row>) rowIterator -> {
                    JOpenCageGeocoder jOpenCageGeocoder = new JOpenCageGeocoder(GEOCODER_API_KEY);
                    List<Row> result = new ArrayList<>();

                    while(rowIterator.hasNext()) {
                        Row row = rowIterator.next();
                        double lat = Double.parseDouble(row.getAs("Latitude"));
                        double lng = Double.parseDouble(row.getAs("Longitude"));
                        Address address = cageGeocoderUtil.getAddress(jOpenCageGeocoder, lat, lng);

                        boolean correctAddr = isCorrectCountry(row, address) && isCorrectCity(row, address) && isCorrectAddress(row, address);

                        if(correctAddr) {
                            result.add(row);
                        }
                    }

                    return result.iterator();
                },
                RowEncoder.apply(schema));
    }

    private boolean isCorrectCity(Row row, Address address) {
        return address.getCity().equalsIgnoreCase(row.getAs("City"));
    }

    private boolean isCorrectCountry(Row row, Address address) {
        return address.getCountryCode().equalsIgnoreCase(row.getAs("Country"));
    }

    private boolean isCorrectAddress(Row row, Address address) {
        String housePlusStreet = String.format("%s %s", address.getHouseNumber(), address.getRoad());
        return housePlusStreet.equalsIgnoreCase(row.getAs("Address"));
    }

    public Dataset<Row> generateGeoHash(Dataset<Row> dataset, String hashColumnName, String latColumnName, String lngColumnName) {
        return dataset.withColumn(
                hashColumnName,
                UdfUtil.getGeohashGenerator().apply(
                        col(latColumnName).cast(DataTypes.DoubleType),
                        col(lngColumnName).cast(DataTypes.DoubleType)));
    }

    public Dataset<Row> joinDatasets(Dataset<Row> leftDataset, Dataset<Row> rightDataset, String joinColumn, String... dropDuplicatesColumns) {
        return leftDataset.join(rightDataset, joinColumn).dropDuplicates(dropDuplicatesColumns);
    }

    public void writeDatasetAsParquet(Dataset<Row> dataset, String path, String... partitions) {
        dataset.write().mode("overwrite").partitionBy(partitions).parquet(path);
    }
}
