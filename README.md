# amazon_transcribe_post_call_analytics
This repository contains a project designed to implement a process for utilizing Amazon Transcribe in post-call analytics for call centre. 
For full tutorial explanation on Amazon Transcribe post call analytics; read this
[article](https://medium.com/@sultanawar321/from-calls-to-data-analytics-unleashing-the-power-of-amazon-transcribe-in-your-call-center-ee9d33937670).

## The repo includes two .py files in the src folder:

- 1) transcribe_jobs_results.py: It has Python module which uses the Transcribe API to initiate and start transcription & analytics jobs.

- 2) transcribe_jobs_results.py: It has Python module which extracts Post Call Analytics results from transcribe jobs. The analytics include transcripts, sentiment analysis, call characteristics, call summarization, and call categorization.

## To run the modules
- python transcribe_jobs_results.py
- pyhton transcribe_jobs_results.py

## Python Packaging:
- Python 3.8.10
- pandas==1.1.5
- boto3==1.23.10
- requests==2.27.1
- retry==0.9.2
- loguru==0.7.2
