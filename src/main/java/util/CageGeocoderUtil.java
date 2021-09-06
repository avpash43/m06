package util;

import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageComponents;
import com.byteowls.jopencage.model.JOpenCageResponse;
import com.byteowls.jopencage.model.JOpenCageReverseRequest;
import model.Address;

import java.io.Serializable;
import java.util.Optional;

public class CageGeocoderUtil implements Serializable {
    public Address getAddress(JOpenCageGeocoder jOpenCageGeocoder, double lat, double lon) {
        JOpenCageReverseRequest request = new JOpenCageReverseRequest(lat, lon);
        request.setLanguage("en");
        request.setNoDedupe(true);
        request.setNoAnnotations(true);

        JOpenCageResponse response = jOpenCageGeocoder.reverse(request);

        if (response.getStatus().getCode() == 200 && response.getResults().size() > 0) {
            JOpenCageComponents components = response.getResults().get(0).getComponents();

            return Address.builder()
                    .countryCode(Optional.ofNullable(components.getCountryCode()).orElse(""))
                    .city(Optional.ofNullable(components.getTown()).orElse(""))
                    .houseNumber(Optional.ofNullable(components.getHouseNumber()).orElse(""))
                    .road(Optional.ofNullable(components.getRoad()).orElse(""))
                    .build();
        } else {
            return Address.builder()
                    .countryCode("")
                    .city("")
                    .build();
        }
    }
}
