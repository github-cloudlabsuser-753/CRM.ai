from azure.cosmos import CosmosClient, PartitionKey
import os
from datetime import datetime
import re


class CosmosDBIngestor:
    def __init__(self):

        # initialize cosmos db client
        self.cosmosdb_endpoint = os.environ.get("COSMOSDB_ENDPOINT")
        self.cosmosdb_key = os.environ.get("COSMOSDB_KEY")
        self.cosmosdb_database_name = "raw_transcripts_db"
        self.cosmosdb_container_name = "raw_transcripts_container"

        self.client = CosmosClient(self.cosmosdb_endpoint, self.cosmosdb_key)
        self.database = self.client.create_database_if_not_exists(id=self.cosmosdb_database_name)
        self.container = self.database.create_container_if_not_exists(
            id=self.cosmosdb_container_name,
            partition_key=PartitionKey(path="/call_id"),
            offer_throughput= 400
        )

    @staticmethod
    def anonymous_pii(text):
        email_regex = r'\b[A-Za-z0-9,_%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        phone_regex = r'^(?:(?:\+|0{0,2})91(\s*[\-]\s*)?|[0]?)?[789]\d{9}$'

        lines = text.split('\n')

        anonymized_lines = []
        for line in lines:
            line = re.sub(email_regex, '[EMAIL]', line)
            line = re.sub(phone_regex, '[PHONE]', line)

            anonymized_lines.append(line)
            anonymized_text = '\n'.join(anonymized_lines)

            return anonymized_text
        
    def ingest_call_transcript(self, transcript):
        try:
            fields = transcript.split(',')
            anonymized_text = self.anonymous_pii(fields[3])

            self.container.create_item(body={
                "conversation_id": fields[0],
                "speaker":fields[1],
                "date_time": fields[2],
                "text": anonymized_text
            })
            print(f"call transcripts for {fields[0]} ingested successfully.")
        except Exception as e:
            print(f"Error ingesting transcripts for {fields[0]}: {str(e)}")

