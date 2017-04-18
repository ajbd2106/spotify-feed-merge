#!/usr/bin/env python

import apache_beam
import configobj
import os

class SetPipelineOptions():
    config = configobj.ConfigObj(os.getcwd()+"/sfm.conf")
    options = apache_beam.utils.pipeline_options.PipelineOptions

    def __init__(self):
        self.google_cloud = SetPipelineOptions.config.get("google_cloud")
        self.runner = SetPipelineOptions.config.get('standard').get('runner')

    def set_google_cloud_options(self, google_cloud, pipeline):
        options = pipeline.view_as(apache_beam.utils.pipeline_options.GoogleCloudOptions) 
        options.project = google_cloud.get("project") 
        options.staging_location = google_cloud.get("staging") 
        options.temp_location = google_cloud.get("temp") 
        return options 

    def set_pipeline(self):
        return apache_beam.utils.pipeline_options.PipelineOptions()

    def set_runner(self, options, runner):
        options = options.view_as(apache_beam.utils.pipeline_options.StandardOptions)
        options.runner = runner 
        return options
