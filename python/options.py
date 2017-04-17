#!/usr/bin/env python

import apache_beam
import configobj
import os


class SetOptions:
    def __init__(self):
        config = configobj.ConfigObj(os.getcwd()+"/sfm.conf")
        google_cloud = configobj.get("google_cloud")
        runner = configobj.get("standard").get("runner")
        options = apache_beam.utils.pipeline_options.PipelineOptions()
        options = set_google_cloud_options(options google_cloud)
        options = set_runner(options, runner)
        return options 

    def set_google_cloud_options(self, options, google_cloud):
        google_cloud_options = options.view_as(apache_beam.utils.pipelne_options.GoogleCloudOptions) 
        google_cloud_options.project = google_cloud.get("project") 
        google_cloud_options.staging_location = google_cloud.get("staging") 
        google_cloud_options.temp_location = google_cloud.get("temp") 
        return google_cloud_options

    def set_runner(self, options, runner):
        options = options.view_as(ab.utils.pipeline_options.StandardOptions)
        options.runner = 'DataflowRunner'
        return options
