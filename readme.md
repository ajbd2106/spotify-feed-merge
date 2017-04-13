# Universal Music Group Technical Test

>  ~~Curent status is that the Java implementation is just about all the way done and I have yet to start the Python implementation which I am aiming at completing by Monday.~~
> # Java works!
> # So does Python!

* Repository for virtual machine deployment along with program code related to the Universal Music Group Technical Test.
* Includes a Vagrantfile for spinning up an Ubuntu 16.04 VM with (most of) the required software installed.
    * Absent the automated installation process are Java and the gcloud libraries along with gradle.
    * The Python compile in the Vagrantfile's VM (/opt/python) is. . . munged. 
    * If you care to try the thing on the environment I built it on, you should use /usr/bin/python, or compile a new one. 

```python
#!/usr/bin/env/ python

data = {
  "user_id":"abbc33a8e91dbe0e55539808467cbac4",
  "isrc":"USC7R1200018",
  "album_code":"00602537151516",
  "product":null,"country":
  "CO","region":"",
  "zip_code":"No",
  "access":"free",
  "gender":"male",
  "partner":"",
  "referral":"",
  "type":"ad",
  "birth_year":"1985",
  "cached":"",
  "timestamp":"2016-10-19 04:30:00 UTC",
  "source_uri":"",
  "track_id":"7b1adc575bd84121b174d45fcd7cd4ae",
  "source":"artist","length":"214",
  "version":"2",
  "device_type":"desktop",
  "message":"APIStreamData",
  "os":"Windows",
  "stream_country":"CO",
  "report_date":"2016-10-19 00:00:00 UTC"
}
```

> One notices a striking similarity between this Python dictionary and the raw JSON.  Hm.

## Tools

### Google BigQuery

### Apache Beam (Google DataFlow)


# Instructions

## Preparation
* ~~Create account on Google Cloud Platform.~~
    * To get one, go to cloud.google.com and login with your Gmail email account. 
    * If you don’t have Gmail email account, register for one. 
    * You’ll be given a trial period and $300 in free spending.
    * [UMG Technical Evaluation](https://console.cloud.google.com/home/dashboard?project=umg-technical-evaluation)
* ~~Create a new bucket on Google Cloud Storage and upload provided source files into it.~~
    * [Young Frankenstein?](https://console.cloud.google.com/storage/browser/abbynormal/?project=umg-technical-evaluation)
* ~~Create a new bucket on Google Cloud Storage for staging files.~~ 
    * You will need it to run DataFlow jobs.
    * [Staging](https://console.cloud.google.com/storage/browser/abbynormal-staging/?project=umg-technical-evaluation)

    * API Key: AIzaSyCzNXpDb0tgTJaHBAZPqP5-xDk5_LvkW8s
* Create two solutions: one for Java SDK and one for Python SDK. 
    * It’s up to you what IDE and configuration to use. 
        * A recent version of BASH and Vim, obviously.
    * Java Development to require [installation](https://github.com/alexander-laughlin/java).
    * At UMG we are using Intellij IDEA + Gradle for Java SDK and Intellij PyCharm IDE for Python.
        * Cool.
* ~~Try examples provided with SDKs to make sure that your IDE environments is configured properly.~~ 
    * Try in in both DirectPipelineRunner and DataflowPipelineRunner (Java).
    * Also try BlockingDataflowPipelineRunner (Python).
    * Java SDK examples:
        * https://github.com/GoogleCloudPlatform/DataflowJavaSDK/tree/master/examples
    * Python SDK examples:
        * https://github.com/apache/incubator-beam/tree/python-sdk/sdks/python 
        * https://github.com/apache/incubator-beam/tree/python-sdk/sdks/python/apache_beam/examples

### Java SDK Tips
* Use TextIO.Read to read files from Google Cloud Storage.
* Join files with CoGroupByKey transformation. Example of this transformation is provided in TfIdf example.
    * [TfIdf on GitHub](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/complete/TfIdf.java)
* Use TextIO.Write to write resulting denormalized file to Google Cloud Storage

### Python SDK Tips

* Configure your Python module same way as in JuliaSet example:
    * [JuliaSet](https://github.com/apache/incubator-beam/tree/python-sdk/sdks/python/apache_beam/examples/complete/juliaset)
* Use Read(io.TextFileSource()) to read files from Google Cloud Storage.
* Use CoGroupByKey() transformation to join files together. 
    * Example of CoGroupByKey for Python is provided in the repository:
        * [CoGroupByKey](https://github.com/apache/incubator-beam/blob/python-sdk/sdks/python/apache_beam/examples/complete/tfidf.py)
* Write demormalized file with Write(io.TextFileSink()) to Google Cloud Storage.
* Approximate number of code lines for Java solution is 350 lines and for Python solution around 100 lines.
    * There will be some routine tasks such as converting from/to JSON, dealing with key/value pairs for joining data, etc. 
    * We leave this to your expertise to figure out how to do it.

# Good luck!

> Thanks! I'm gonna need it.
