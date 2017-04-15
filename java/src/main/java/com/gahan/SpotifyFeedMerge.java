package com.gahan;

import com.google.cloud.dataflow.sdk.*;
import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.io.*;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.runners.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.*;
import com.google.cloud.dataflow.sdk.values.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

import org.slf4j.*;

import java.io.*;
import java.util.*;

public class SpotifyFeedMerge {
  private static final Logger LOG = LoggerFactory.getLogger(SpotifyFeedMerge.class);

  public static void SpotifyFeedMerge() {}

  public static class ReadStreams
      extends PTransform<PInput, PCollection<KV<String, String>>> {

    public static void ReadStreams() {}

    public PCollection<KV<String, String>> apply(PInput input) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

      Pipeline pipeline = input.getPipeline();

      PCollection<String> streams = pipeline
          .apply(TextIO.Read.from("gs://sfm-bucket/streams.gz"));

      PCollection<KV<String, String>> streamsKeyValue = streams 
        .apply(ParDo.named("makeStreamsKeyValue").of(
          new DoFn<String, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                StreamData valuesJson = mapper.readValue(c.element().toString(), StreamData.class);
                JsonNode root = mapper.readTree(c.element().toString());
                String key = root.get("user_id").asText();
                String streamsString = mapper.writeValueAsString(root);
                c.output(KV.of(key, streamsString));
              }
              catch (IOException e) {
                LOG.info(e.toString());
              }
            }
          }
        ));

      return streamsKeyValue;
    }

  }

  /*

      final TupleTag<String> tracksTag = new TupleTag<String>();
      final TupleTag<String> suTag= new TupleTag<String>();
      KeyedPCollectionTuple<String> coGetbkInput = KeyedPCollectionTuple
        .of(suTag, streamsUsers)
        .and(tracksTag, this.tracks);

      PCollection<KV<String, CoGbkResult>> streamsTracksGroupBy = coGetbkInput
        .apply("CoGroupByTrackId", CoGroupByKey.<String>create());
      
      PCollection<String> streamsTracks = streamsTracksGroupBy 
        .apply(ParDo.named("streamsTracks").of(
          new DoFn<KV<String, CoGbkResult>, String>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                String streamValue = c.element().getValue().getAll(suTag).toString();
                List<StreamData> streams = mapper.readValue(streamValue, new TypeReference<List<StreamData>>(){});
                for (int i = 0; i < streams.size(); i++) {
                  String streamString = mapper.writeValueAsString(streams.get(i));
                  StreamData stream = mapper.readValue(streamString, StreamData.class);
                  JsonNode streamJson = mapper.readTree(streamString);
                  String trackValue = c.element().getValue().getAll(tracksTag).toString();
                  //LOG.info("\n\n\ntrackValue\n\n\n"+trackValue);
                  List<StreamData> tracks = mapper.readValue(trackValue, new TypeReference<List<StreamData>>(){}); 
                  for (int j = 0; j < tracks.size(); j++) {
                    String trackString = mapper.writeValueAsString(tracks.get(j));
                    StreamData track = mapper.readValue(trackString, StreamData.class);
                    JsonNode trackJson = mapper.readTree(trackString);
                    //LOG.info("\n\n\nstreamJson\n\n\n"+mapper.writeValueAsString(streamJson));
                    //LOG.info("\n\n\ntrackJson\n\n\n"+mapper.writeValueAsString(trackJson));
                    ((ObjectNode) streamJson).put("album_code",trackJson.get("album_code").asText());
                    ((ObjectNode) streamJson).put("isrc",trackJson.get("isrc").asText());
                    String key = c.element().getKey();
                    c.output(mapper.writeValueAsString(streamJson));
                  }
                }
              }
              catch (IOException e) {
                LOG.info(e.toString());
              }
            }
          }
        ));

    
      return streamsTracks; 
    }
  }
  */

  public static class ReadTracks
    extends PTransform<PInput, PCollection<KV<String, String>>> {

    public ReadTracks() {}

    public PCollection<KV<String, String>> apply(PInput input) {
      ObjectMapper mapper = new ObjectMapper();
      Pipeline pipeline = input.getPipeline();

      PCollection<String> tracks = pipeline
        .apply(TextIO.Read.from("gs://sfm-bucket/tracks.gz"));

      PCollection<KV<String, String>> tracksKeyValue = tracks
        .apply(ParDo.named("makeTracksKeyValue").of(
          new DoFn<String, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                //LOG.info(c.element().toString());  
                StreamData tracksJson = mapper.readValue(c.element().toString(), StreamData.class);
                JsonNode tracksRoot = mapper.readTree(c.element().toString());
                String key = tracksRoot.get("track_id").asText();
                //LOG.info(mapper.writeValueAsString(tracksJson));
                c.output(KV.of(key, mapper.writeValueAsString(tracksJson)));
              }
              catch (IOException e) {
                LOG.info(e.toString());
            }
            }
          }
        )
      );
      return tracksKeyValue;
    }
  }

  public static class ReadUsers
    extends PTransform<PInput, PCollection<KV<String, String>>> {

    public ReadUsers() {}

    public PCollection<KV<String, String>> apply(PInput input) {
      ObjectMapper mapper = new ObjectMapper();
      Pipeline pipeline = input.getPipeline();     
 
      PCollection<String> users = pipeline
        .apply(TextIO.Read.from("gs://sfm-bucket/users.gz"));

      PCollection<KV<String, String>> usersKeyValue = users
        .apply(ParDo.named("makeUsersKeyValue").of(
          new DoFn<String, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                StreamData usersJson = mapper.readValue(c.element().toString(), StreamData.class);
                JsonNode userRoot = mapper.readTree(c.element().toString());
                String key = userRoot.get("user_id").asText();
                String usersString = mapper.writeValueAsString(userRoot);
                c.output(KV.of(key, usersString));
              }
              catch (IOException e) {
                LOG.info(e.toString());
              }
            }
          }
        )
      );
      return usersKeyValue;
    }
  }

  public static class TransformStreamsUsers
    extends PTransform<PCollectionList<KV<String, String>>, PCollection<KV<String, String>>> {

    public PCollection<KV<String, String>> apply(PCollectionList<KV<String, String>>input) {
      ObjectMapper mapper = new ObjectMapper();
      Pipeline p = input.getPipeline();
      KV<String, String> keyValue = KV.of("userid","value"); 

      PCollection<KV<String, String>> streams = input.get(0);
      PCollection<KV<String, String>> users = input.get(1);

      final TupleTag<String> streamsTag = new TupleTag<String>();
      LOG.info(streamsTag.toString());
      final TupleTag<String> usersTag = new TupleTag<String>();
      KeyedPCollectionTuple<String> coGbkInput = KeyedPCollectionTuple
          .of(streamsTag, streams)
          .and(usersTag, users);

      LOG.info(p.toString());
      LOG.info(keyValue.toString());
      PCollection<KV<String, String>> returnCollection = p.apply(Create.of(keyValue)); 
      LOG.info(returnCollection.toString());
      return returnCollection;
    }

    /*(public PCollection<KV<String, String>> apply(PInput input) {
      ObjectMapper mapper = new ObjectMapper();
      Pipeline pipeline = input.getPipeline();

      LOG.info(pipeline.toString());

      /*final TupleTag<String> streamsTag = new TupleTag<String>();
      final TupleTag<String> usersTag = new TupleTag<String>();
      KeyedPCollectionTuple<String> coGbkInput = KeyedPCollectionTuple
          .of(streamsTag, streams)
          .and(usersTag, users);
      PCollection<KV<String, CoGbkResult>> streamsUsersGroupBy = coGbkInput
          .apply("CoGroupByUserId", CoGroupByKey.<String>create());

      /*
      PCollection<KV<String, String>> streamsUsers = streamsUsersGroupBy
        .apply(ParDo.named("groupStreamsUsers").of(
          new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              //try {
              //LOG.info(c.element().toString());
                //String userString = c.element().getValue().getOnly(usersTag).toString(); 
                //LOG.info(userString.toString());
                //StreamData user = mapper.readValue(userString, StreamData.class);
                /*
                JsonNode uRoot = mapper.readTree(userString);
                String userString = mapper.writeValueAsString(user);

                String streamValue = c.element().getValue().getAll(streamsTag).toString();
                List<StreamData> streams = mapper.readValue(streamValue, new TypeReference<List<StreamData>>(){});
                for (int i = 0; i < streams.size(); i++) {
                  StreamData stream = streams.get(i);
                  JsonNode streamJson = mapper.readTree(mapper.writeValueAsString(stream));
                  ((ObjectNode) streamJson).put("access",uRoot.get("access").asText());
                  ((ObjectNode) streamJson).put("birth_year",uRoot.get("birth_year").asText());
                  ((ObjectNode) streamJson).put("country",uRoot.get("country").asText());
                  ((ObjectNode) streamJson).put("gender",uRoot.get("gender").asText());
                  ((ObjectNode) streamJson).put("partner",uRoot.get("partner").asText());
                  ((ObjectNode) streamJson).put("referral",uRoot.get("referral").asText());
                  ((ObjectNode) streamJson).put("region",uRoot.get("region").asText());
                  ((ObjectNode) streamJson).put("type",uRoot.get("type").asText());
                  ((ObjectNode) streamJson).put("zip_code",uRoot.get("zip_code").asText());
                  String key = streamJson.get("track_id").asText(); 
                  c.output(KV.of(key,mapper.writeValueAsString(streamJson)));
                }
                */
              //}
              //catch (IOException e) {
               // LOG.info(c.toString());
                //LOG.info(e.toString());
              /*}
            }
          }
        )
      );
      //return streamsUsers;
      return this.streams;
    }
      */
  }

  public static void main(String[] args) throws Exception {
    //DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    DirectPipelineOptions options = PipelineOptionsFactory.as(DirectPipelineOptions.class);

    // Set options for the Dataflow Pipeline
    //options.setRunner(DataflowPipelineRunner.class);
    options.setRunner(DirectPipelineRunner.class);
    options.setProject("spotify-feed-merge");
    //options.setStagingLocation("gs://sfm-staging");

    // Create the pipeline and set a registry.
    Pipeline pipeline = Pipeline.create(options);
    pipeline.getCoderRegistry().registerCoder(String.class, StringDelegateCoder.of(String.class));

    PCollection<KV<String, String>> streams = pipeline
      .apply(new ReadStreams());

    PCollection<KV<String, String>> users = streams
      .apply(new ReadUsers()); 

    PCollectionList<KV<String, String>> list = PCollectionList.of(streams).and(users);

    list
      .apply(new TransformStreamsUsers());
    //  kvs
    //    .apply(TextIO.Write.named("WriteIt").to("gs://sfm-bucket/merged").withSuffix(".json"));

    pipeline.run();
  }
}
