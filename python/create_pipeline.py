#!/usr/bin/env python

import apache_beam

class CreatePipeline(apache_beam.Pipeline):
    pipeline = []

    def __init__(self, options):
        self.pipeline = apache_beam.Pipeline(options=options)
