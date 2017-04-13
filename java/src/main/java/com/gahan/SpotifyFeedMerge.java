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
  private static final Logger LOG = LoggerFactory.getLogger(DPRunner.class);

  public static class ReadStreams
      extends PTransform<PInput, PCollection<String>> {

    public ReadStreams() {}

    public PCollection<String> apply(PInput input) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

      Pipeline pipeline = input.getPipeline();

      PCollection<String> valueString = pipeline
          .apply(TextIO.Read.from("gs://abbynormal/streams.gz"));

      LOG.info(pipeline.toString());
      LOG.info(valueString.toString());
    
      PCollection<KV<String, String>> streamskv = valueString 
        .apply(ParDo.named("streamsKeyValue").of(
          new DoFn<String, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                streamData valuesJson = mapper.readValue(c.element().toString(), streamData.class);
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

      PCollection<String> users = pipeline
        .apply(TextIO.Read.from("gs://abbynormal/users.gz"));

      PCollection<KV<String, String>> userskv = users
        .apply(ParDo.named("UsersKV").of(
          new DoFn<String, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                streamData usersJson = mapper.readValue(c.element().toString(), streamData.class);
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
        ));

      final TupleTag<String> streamsTag = new TupleTag<String>();
      final TupleTag<String> usersTag = new TupleTag<String>();
      KeyedPCollectionTuple<String> coGbkInput = KeyedPCollectionTuple
          .of(streamsTag, streamskv)
          .and(usersTag, userskv);

      PCollection<KV<String, CoGbkResult>> streamsUsersGroupBy = coGbkInput
          .apply("CoGroupByUserId", CoGroupByKey.<String>create());

      PCollection<KV<String, String>> streamsUsers = streamsUsersGroupBy
        .apply(ParDo.named("streamsUsers").of(
          new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                String userValue = c.element().getValue().getOnly(usersTag).toString(); 
                streamData user = mapper.readValue(userValue, streamData.class);
                JsonNode uRoot = mapper.readTree(userValue);
                String userString = mapper.writeValueAsString(user);

                String streamValue = c.element().getValue().getAll(streamsTag).toString();
                List<streamData> streams = mapper.readValue(streamValue, new TypeReference<List<streamData>>(){});
                for (int i = 0; i < streams.size(); i++) {
                  streamData stream = streams.get(i);
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
                LOG.info(e.toString());
              }
            }
          }
        ));

      PCollection<String> tracks = pipeline
        .apply(TextIO.Read.from("gs://abbynormal/tracks.gz"));

      PCollection<KV<String, String>> trackskv = tracks
        .apply(ParDo.named("TracksKV").of(
          new DoFn<String, KV<String, String>>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                //LOG.info(c.element().toString());  
                streamData tracksJson = mapper.readValue(c.element().toString(), streamData.class);
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
        ));

      final TupleTag<String> tracksTag = new TupleTag<String>();
      final TupleTag<String> suTag= new TupleTag<String>();
      KeyedPCollectionTuple<String> coGetbkInput = KeyedPCollectionTuple
        .of(suTag, streamsUsers)
        .and(tracksTag, trackskv);

      PCollection<KV<String, CoGbkResult>> streamsTracksGroupBy = coGetbkInput
        .apply("CoGroupByTrackId", CoGroupByKey.<String>create());
      
      PCollection<String> streamsTracks = streamsTracksGroupBy 
        .apply(ParDo.named("streamsTracks").of(
          new DoFn<KV<String, CoGbkResult>, String>() {
            @Override
            public void processElement(ProcessContext c) {
              try {
                String streamValue = c.element().getValue().getAll(suTag).toString();
                List<streamData> streams = mapper.readValue(streamValue, new TypeReference<List<streamData>>(){});
                for (int i = 0; i < streams.size(); i++) {
                  String streamString = mapper.writeValueAsString(streams.get(i));
                  streamData stream = mapper.readValue(streamString, streamData.class);
                  JsonNode streamJson = mapper.readTree(streamString);
                  String trackValue = c.element().getValue().getAll(tracksTag).toString();
                  //LOG.info("\n\n\ntrackValue\n\n\n"+trackValue);
                  List<streamData> tracks = mapper.readValue(trackValue, new TypeReference<List<streamData>>(){}); 
                  for (int j = 0; j < tracks.size(); j++) {
                    String trackString = mapper.writeValueAsString(tracks.get(j));
                    streamData track = mapper.readValue(trackString, streamData.class);
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

  public static void DPRunner() {}

  public static void main(String[] args) throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    //DirectPipelineOptions options = PipelineOptionsFactory.as(DirectPipelineOptions.class);
    //options.setRunner(DirectPipelineRunner.class);
    options.setRunner(DataflowPipelineRunner.class);
    options.setProject("umg-technical-evaluation");
    options.setStagingLocation("gs://abbynormal-staging");
    Pipeline pipeline = Pipeline.create(options);
    pipeline.getCoderRegistry().registerCoder(String.class, StringDelegateCoder.of(String.class));

    PCollection<String> kvs = pipeline
        .apply(new ReadStreams());

      kvs
        .apply(TextIO.Write.named("WriteIt").to("gs://abbynormal/denormal").withSuffix(".json"));

    pipeline.run();
  }
}
