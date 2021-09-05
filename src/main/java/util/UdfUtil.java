package util;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

public class UdfUtil {
    private static final int NUMBER_OF_GEOHASH_CHARACTERS = 4;

    public static UserDefinedFunction getGeohashGenerator() {
        return udf((Double lat, Double lng) ->
                GeoHash.geoHashStringWithCharacterPrecision(lat, lng, NUMBER_OF_GEOHASH_CHARACTERS), DataTypes.StringType
        );
    }
}
