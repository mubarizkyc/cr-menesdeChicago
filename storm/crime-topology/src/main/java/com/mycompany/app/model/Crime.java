package com.mycompany.app.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Crime implements Serializable {
    
    @JsonProperty("ID") private String id;
    @JsonProperty("Case Number") private String caseNumber;
    @JsonProperty("Date") private String date;
    @JsonProperty("Block") private String block;
    @JsonProperty("IUCR") private String iucr;
    @JsonProperty("Primary Type") private String primaryType;
    @JsonProperty("Description") private String description;
    @JsonProperty("Location Description") private String locationDescription;
    @JsonProperty("Arrest") private String arrest;
    @JsonProperty("Domestic") private String domestic;
    @JsonProperty("Beat") private String beat;
    @JsonProperty("District") private String district;
    @JsonProperty("Ward") private String ward;
    @JsonProperty("Community Area") private String communityArea;
    @JsonProperty("FBI Code") private String fbiCode;
    @JsonProperty("X Coordinate") private String xCoordinate;
    @JsonProperty("Y Coordinate") private String yCoordinate;
    @JsonProperty("Year") private String year;
    @JsonProperty("Updated On") private String updatedOn;
    @JsonProperty("Latitude") private String latitude;
    @JsonProperty("Longitude") private String longitude;
    @JsonProperty("Location") private String location;

    public Crime() {}
    
    public String getDistrict() { return district; }
    public String getId() { return id; }
    public String getPrimaryType() { return primaryType; }
    
    @Override
    public String toString() {
        return "Crime{id='" + id + "', district='" + district + "', type='" + primaryType + "'}";
    }
}
