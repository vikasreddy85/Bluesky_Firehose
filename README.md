# Bluesky_Firehose

## Scripts
* websocket.py
    * Reads data from the US-East Jetstream network in real-time.
    * Writes the incoming data to disk and compresses the JSON files every hour.

* preprocess.py
    * Reads data from the compressed JSON files created by websocket.py.
    * Places the data into a queue for processing.
    * Processes each file in the queue to extract referenced reposts or likes.
    * Writes the processed payloads to a separate file for further analysis.

* data_processing.py
    * Takes the output files from websocket.py and preprocess.py.
    * Extracts all URLs from the data and resolves shortened URLs.
    * Analyzes the URLs using NewsGuard ratings:
        * Trustworthy: NewsGuard score ≥ 60.
        * Untrustworthy: NewsGuard score < 60.

* orchestrator.py
    * Integrates the logic of all scripts.
    * Ensures a smooth end-to-end data processing pipeline.

## Examples
* urls-per_minute
    * Displays all URLs processed for a specific minute from Bluesky data.

* Unprocessed and Processed Payloads
    * Example JSON objects include:
        * Unprocessed Payloads: Raw repost or like data.
        * Processed Payloads: Repost and like data extracted from the raw payloads.