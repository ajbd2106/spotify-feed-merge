#!/usr/bin/env python

import apache_beam
import configobj
import os


class SetOptions:
    project = ""
    staging = ""
    temp = ""

    def __init__(self):
        config = configobj.ConfigObj(os.getcwd()+"/sfm.conf")
        dataflow = configobj.get("dataflow")
        options = apache_beam.utils.pipeline_options.PipelineOptions()
        pass

    def set_google_cloud_options(self, options):
        google_cloud_options = options.view_as(apache_beam.utils.pipelne_options.GoogleCloudOptions) 
        gco.project = 'umg-technical-evaluation'
gco.staging_location = 'gs://abbynormal-staging/stage'
gco.temp_location = 'gs://abbynormal-staging/tmp'
        return google_cloud_options

print(config)

o = ab.utils.pipeline_options.PipelineOptions()
gco = o.view_as(ab.utils.pipeline_options.GoogleCloudOptions)
o.view_as(ab.utils.pipeline_options.StandardOptions).runner = 'DataflowRunner'
