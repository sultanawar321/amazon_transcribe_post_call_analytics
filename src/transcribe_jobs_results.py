"""
This Python module uses the Post Call Analytics feature from Amazon Transcribe 
to extract analytics results from transcribe jobs. The analytics include sentiment analysis, call characteristics,
call summarization, and call categorization.

It calls the TRANSCRIBE.get_call_analytics_job API to retrieve and extract the analytics results.
"""

# import packages
import pandas as pd
import boto3
import requests as r
from loguru import logger
from retry import retry


@retry(tries=5)
def analytics_job_response_output(df_jobs, transcribe):
    """
    Retrieves and parses the output of call analytics jobs.

    Args:
    - df (pd.DataFrame): DataFrame containing job information with the 'job_name' column.
    - transcribe (boto3.client): Boto3 client for the Amazon Transcribe service.

    Returns:
    pd.DataFrame: Updated DataFrame with job response output.
    """

    def job_func(row):
        job_name = row["job_name"]
        try:
            row["job_response_output"] = r.get(
                transcribe.get_call_analytics_job(CallAnalyticsJobName=job_name)[
                    "CallAnalyticsJob"
                ]["Transcript"]["TranscriptFileUri"]
            ).json()
        except Exception as error:
            logger.error(f"Failed to retrieve job response for job {job_name}: {error}")
            row["job_response_output"] = None
        return row

    df_jobs_results = df_jobs.apply(job_func, axis=1)
    incomplete_jobs = df_jobs_results["job_response_output"].isnull().sum()
    logger.info(f"Number of incomplete jobs: {incomplete_jobs}")
    return df_jobs_results


def analytics_call_transcript(row):
    """
    Parses the transcript datapoint from the job output of call analytics.

    Args:
    - row (pd.Series): DataFrame row containing the 'job_response_output' column.

    Returns:
    pd.Series: Updated row with the parsed transcript added as the 'transcript' column
    """
    speakers = ["Speaker 1", "Speaker 2"]
    try:
        for i in row["job_response_output"]:
            transcript_dict = i.get("Transcript", {})
            if transcript_dict:
                content_list = [
                    dict_result.get("Content", "") for dict_result in transcript_dict
                ]
                transcript = [
                    f"{speakers[i % len(speakers)]}: {text}"
                    for i, text in enumerate(content_list)
                ]
            row["transcript"] = ", ".join(transcript)
    except:
        row["transcript"] = None
    return row


def transcript_call_public_fnt(tmp) -> pd.DataFrame:
    """
    Processes a DataFrame containing job response outputs and generates
    transcripts using the 'analytics_call_transcript' function
    Args:
    - tmp (pd.DataFrame): DataFrame containing job response outputs.

    Returns:
    pd.DataFrame: DataFrame with transcripts extracted from the job response outputs.
    """
    results_df = pd.DataFrame()
    for i in range(0, len(tmp)):
        try:
            row_df = tmp.loc[[i], :].copy()
            row_df_results = analytics_call_transcript(row_df)
            results_df = pd.concat([results_df, row_df_results])
        except:
            pass
    results_df = results_df.reset_index(drop=True)
    empty_transcripts = results_df["transcript"].isnull().sum()
    logger.info(f"Number of empty transcripts: {empty_transcripts}")
    return results_df


def analytics_call_summarization(row):
    """
    Parses summarization datapoints from the job response output.
    Args:
    - row (pd.Series): DataFrame row containing the 'job_response_output' column.

    Returns:
    pd.Series: Updated row with the parsed summarization data.
    """
    key_list = []
    content_list = []
    key_cols = ["IssuesDetected", "OutcomesDetected", "ActionItemsDetected"]
    df_summary = pd.DataFrame()
    try:
        for i in row["job_response_output"]:
            transcript = i["Transcript"]
            for dict_result in transcript:
                keys = dict_result.keys()
                for k in keys:
                    if k in key_cols:
                        key_list.append(k)
                        content_list.append(dict_result["Content"])
        if key_list:
            data = {name: [] for name in key_list}
            for i, val in enumerate(content_list):
                data[key_list[i % len(key_list)]].append(val)
            df_summary = pd.DataFrame(data)
    except:
        df_summary = pd.DataFrame(columns=key_cols)
    for col in key_cols:
        if col not in df_summary.columns:
            df_summary[col] = None
    row.index = [0]
    row = pd.concat([row, df_summary], axis=1)
    return row


def summary_call_public_fnt(tmp) -> pd.DataFrame:
    """
    Processes a DataFrame containing job response outputs and generates
    summarization data using the 'analytics_call_summarization' function.
    Args:
    - tmp (pd.DataFrame): DataFrame containing job response outputs.
    Returns:
    pd.DataFrame: DataFrame with summarization data extracted from the job
    response outputs.
    """
    results_df = pd.DataFrame()
    for i in range(0, len(tmp)):
        row_df = tmp.iloc[[i], :].copy()
        row_df_results = analytics_call_summarization(row_df)
        results_df = pd.concat([results_df, row_df_results], axis=0)
        results_df = results_df.reset_index(drop=True)
    return results_df


def parse_call_analytics_output(df) -> pd.DataFrame:

    """
    This function parse each data attribute related to sentiment and call details
    from the job output with try/except approach to account for missing values.
    Returns:
    pd.DataFrame: DataFrame with calls characteristics and sentiment extracted
    from the job response outputs.
    """
    try:
        df["overall_sentiment_customer"] = df["job_response_output"][
            "ConversationCharacteristics"
        ]["Sentiment"]["OverallSentiment"]["CUSTOMER"]
    except:
        df["overall_sentiment_customer"] = None
    try:
        df["overall_sentiment_agent"] = df["job_response_output"][
            "ConversationCharacteristics"
        ]["Sentiment"]["OverallSentiment"]["AGENT"]
    except:
        df["overall_sentiment_agent"] = None
    try:
        df["sentiment_scores_agent_per_quarter"] = [
            d["Score"]
            for d in df["job_response_output"]["ConversationCharacteristics"][
                "Sentiment"
            ]["SentimentByPeriod"]["QUARTER"]["AGENT"]
        ]
    except:
        df["sentiment_scores_agent_per_quarter"] = None
    try:
        df["sentiment_scores_customer_per_quarter"] = [
            d["Score"]
            for d in df["job_response_output"]["ConversationCharacteristics"][
                "Sentiment"
            ]["SentimentByPeriod"]["QUARTER"]["CUSTOMER"]
        ]
    except:
        df["sentiment_scores_customer_per_quarter"] = None
    try:
        df["non_talk_time_sec"] = (
            df["job_response_output"]["ConversationCharacteristics"]["NonTalkTime"][
                "TotalTimeMillis"
            ]
        ) / 1000
    except:
        df["non_talk_time_sec"] = None
    try:
        df["interupted_time_agent_sec"] = (
            df["job_response_output"]["ConversationCharacteristics"]["Interruptions"][
                "InterruptionsByInterrupter"
            ]["AGENT"][0]["DurationMillis"]
        ) / 1000
    except:
        df["interupted_time_agent_sec"] = None
    try:
        df["interupted_time_customer_sec"] = (
            df["job_response_output"]["ConversationCharacteristics"]["Interruptions"][
                "InterruptionsByInterrupter"
            ]["CUSTOMER"][0]["DurationMillis"]
        ) / 1000
    except:
        df["interupted_time_customer_sec"] = None
    try:
        df["talk_speed_words_per_min_agent"] = df["job_response_output"][
            "ConversationCharacteristics"
        ]["TalkSpeed"]["DetailsByParticipant"]["AGENT"]["AverageWordsPerMinute"]
    except:
        df["talk_speed_words_per_min_agent"] = None
    try:
        df["talk_speed_words_per_min_customer"] = df["job_response_output"][
            "ConversationCharacteristics"
        ]["TalkSpeed"]["DetailsByParticipant"]["CUSTOMER"]["AverageWordsPerMinute"]
    except:
        df["talk_speed_words_per_min_customer"] = None
    try:
        df["talk_time_agent_sec"] = (
            df["job_response_output"]["ConversationCharacteristics"]["TalkTime"][
                "DetailsByParticipant"
            ]["AGENT"]["TotalTimeMillis"]
        ) / 1000
    except:
        df["talk_time_agent_sec"] = None
    try:
        df["talk_time_customer_sec"] = (
            df["job_response_output"]["ConversationCharacteristics"]["TalkTime"][
                "DetailsByParticipant"
            ]["CUSTOMER"]["TotalTimeMillis"]
        ) / 1000
    except:
        df["talk_time_customer_sec"] = None
    try:
        df["matched_categories"] = df["job_response_output"]["Categories"][
            "MatchedCategories"
        ]
    except:
        df["matched_categories"] = None
    return df


@retry(tries=5)
def main_analytics_function(df_jobs, transcribe):
    """
    Main function to process call analytics jobs and generate transcripts and summaries.

    Args:
    - df_jobs (pd.DataFrame): DataFrame containing job information with the 'job_name' column.
    - transcribe (boto3.client): Boto3 client for the Amazon Transcribe service.

    Returns:
    pd.DataFrame: Final DataFrame with job responses, transcripts, summaries, and parsed attributes.
    """

    # Step 1: Retrieve and parse the output of call analytics jobs
    df_jobs_with_responses = analytics_job_response_output(df_jobs, transcribe)

    # Step 2: Generate transcripts from the job responses
    df_with_transcripts = transcript_call_public_fnt(df_jobs_with_responses)

    # Step 3: Generate summarization data from the job responses
    df_with_summaries = summary_call_public_fnt(df_with_transcripts)

    # Step 4: Parse additional analytics output such as sentiment and call traits
    final_df = df_with_summaries.apply(parse_call_analytics_output, axis=1)

    return final_df


if __name__ == "__main__":
    # Establish a connection to AWS using a boto3 session with the specified credentials profile
    boto_session = boto3.Session(profile_name="<The credentials profile name>")

    # Create a boto3 resource object for IAM-related operations
    boto_iam = boto_session.resource("iam")

    # Create a boto3 client object for the Amazon Transcribe service
    transcribe_client = boto_session.client("transcribe")

    # Load the data with Transcribe jobs names
    df_calls_jobs = pd.read_csv("recordings.csv")

    # Apply the main analytics function
    result_df = main_analytics_function(df_calls_jobs, transcribe_client)
    logger.info(
        "The completion of transcribe retrieval jobs with calls: {}".format(
            len(df_calls_jobs)
        )
    )
