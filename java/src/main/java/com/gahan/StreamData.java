package com.gahan;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamData {
  public String user_id;
  public String isrc;
  public String album_code;
  public String product;
  public String country;
  public String region;
  public String zip_code;
  public String access;
  public String gender;
  public String partner;
  public String referral;
  public String type; 
  public String birth_year;
  public String cached;
  public String timestamp;
  public String source_uri;
  public String track_id; 
  public String source;
  public String length;
  public String version;
  public String device_type;
  public String message;
  public String os;
  public String report_date;
  public String stream_country;
}
