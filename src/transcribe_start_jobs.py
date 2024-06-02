"""
This Python module uses the Transcribe API to initiate transcription & 
analytics jobs using the following method: <TRANSCRIBE.start_call_analytics_job>
"""
# Import packages
import boto3
from loguru import logger
import pandas as pd
from retry import retry


@retry(tries=5)
def start_analytics_jobs(df_calls, transcribe_client, data_access_role):
    """
    Initiates call analytics jobs using the Amazon Transcribe service.

    This function takes a DataFrame containing job details and calls the Transcribe
    API to start analytics jobs for each entry. It retries up to 5 times in case
    of failures.

    """

    def job_func(row):
        """
        Helper function to start a call analytics job for a given row.

        """
        job_name = row["job_name"]
        job_url = row["job_url"]
        try:
            transcribe_client.start_call_analytics_job(
                CallAnalyticsJobName=job_name,
                Media={"MediaFileUri": job_url},
                DataAccessRoleArn=data_access_role.arn,
                ChannelDefinitions=[
                    {"ChannelId": 0, "ParticipantRole": "CUSTOMER"},
                    {"ChannelId": 1, "ParticipantRole": "AGENT"},
                ],
            )
        except Exception as error:
            print(f"Error starting job {job_name}: {error}")
        return row

    df_calls = df_calls.apply(job_func, axis=1)
    return df_calls


def main():
    """
    Defines main variables and runs the previous function to set up the Transcribe jobs
    """
    # Establish a connection to AWS using a boto3 session with the specified credentials profile
    boto_session = boto3.Session(profile_name="<The credentials profile name>")

    # Create a boto3 resource object for IAM-related operations
    boto_iam = boto_session.resource("iam")

    # Create a boto3 client object for the Amazon Transcribe service
    transcribe_client = boto_session.client("transcribe")

    # Define an IAM Role object that grants necessary permissions for Transcribe to access data
    data_access_role = boto_iam.Role("<The IAM role name>")

    # Load the data with recording urls
    df_calls_jobs = pd.read_csv("recordings.csv")

    df_calls_jobs_output = start_analytics_jobs(
        df_calls_jobs, transcribe_client, data_access_role
    )
    logger.info(
        "The completion of post_call_analytics_jobs notebook with number of calls: {}".format(
            len(df_calls_jobs)
        )
    )
    logger.info("post_call_analytics_jobs: end")
    return df_calls_jobs_output


if __name__ == "__main__":
    main()
