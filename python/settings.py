#!/usr/bin/env python

o = ab.utils.pipeline_options.PipelineOptions()
gco = o.view_as(ab.utils.pipeline_options.GoogleCloudOptions)
gco.project = 'umg-technical-evaluation'
gco.staging_location = 'gs://abbynormal-staging/stage'
gco.temp_location = 'gs://abbynormal-staging/tmp'
o.view_as(ab.utils.pipeline_options.StandardOptions).runner = 'DataflowRunner'
