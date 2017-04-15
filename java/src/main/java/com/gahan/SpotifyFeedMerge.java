package com.gahan;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.Flatten.FlattenIterables;
import com.google.cloud.dataflow.sdk.transforms.Keys;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

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
    extends PTransform<PInput, PCollection<KV<String, String>>> {

    PCollection<KV<String, String>> streamsKeyValue;
    PCollection<KV<String, String>> usersKeyValue;

    public TransformStreamsUsers(PCollection<KV<String, String>> streams, PCollection<KV<String, String>> users) {
      this.streamsKeyValue = streams;
      LOG.info(streams.toString());
      this.usersKeyValue = users;
      LOG.info(users.toString());
    }

    public PCollection<KV<String, String>> apply(PInput input) {
      ObjectMapper mapper = new ObjectMapper();
      Pipeline pipeline = input.getPipeline();

      final TupleTag<String> streamsTag = new TupleTag<String>();
      final TupleTag<String> usersTag = new TupleTag<String>();
      KeyedPCollectionTuple<String> coGbkInput = KeyedPCollectionTuple
          .of(streamsTag, this.streamsKeyValue)
          .and(usersTag, this.usersKeyValue);
      /*
      PCollection<KV<String, CoGbkResult>> streamsUsersGroupBy = coGbkInput
          .apply("CoGroupByUserId", CoGroupByKey.<String>create());

      PCollection<KV<String, String>> streamsUsers = streamsUsersGroupBy
        .apply(ParDo.named("transformStreamsUsers").of(
          new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                String userValue = c.element().getValue().getOnly(usersTag).toString(); 
                StreamData user = mapper.readValue(userValue, StreamData.class);
                JsonNode uRoot = mapper.readTree(userValue);
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
              }
              catch (IOException e) {
                System.out.println("\n\n\nThis is an IOException.\n\n\n");
                LOG.info(c.toString());
                LOG.info(e.toString());
              }
            }
          }
        )
      );
      return streamsUsers;
      */
      return this.streamsKeyValue;
    }
  }

  public static void main(String[] args) throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    // Set options for the Dataflow Pipeline
    options.setRunner(DataflowPipelineRunner.class);
    options.setProject("spotify-feed-merge");
    options.setStagingLocation("gs://sfm-staging");

    // Create the pipeline and set a registry.
    Pipeline pipeline = Pipeline.create(options);
    pipeline.getCoderRegistry().registerCoder(String.class, StringDelegateCoder.of(String.class));

    PCollection<KV<String, String>> streams = pipeline
      .apply(new ReadStreams());
      //.apply(new ReadTracks());

    //PCollection<KV<String, String>> tracks = streams 

    PCollection<KV<String, String>> users = pipeline
      .apply(new ReadUsers());

    streams.apply(new TransformStreamsUsers(streams, users));

    //PCollection<String> kvs = pipeline
    //    .apply(new ReadStreams(tracks));

    //  kvs
    //    .apply(TextIO.Write.named("WriteIt").to("gs://sfm-bucket/merged").withSuffix(".json"));

    pipeline.run();
  }
}
