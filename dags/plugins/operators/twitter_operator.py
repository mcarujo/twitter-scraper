import numpy as np
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.twitter_hook import TwitterHook


class TwitterOperator(BaseOperator):

    template_fields = ["query", "file_path", "start_time", "end_time"]

    @apply_defaults
    def __init__(
        self,
        query,
        file_path,
        conn_id=None,
        start_time=None,
        end_time=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context):
        self.log.info("Starting the twitter operator...")
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time,
        )
        count = 0
        pieces = []
        for pg in hook.run():
            try:
                pieces.append(pg["data"])
                count = count + len(pg["data"])
                self.log.info(f"Number of tweets captured: {count}")
            except:
                self.log.info(f"Stoped the capturation")
                break
        pd.DataFrame(list(np.concatenate(pieces).reshape(-1))).to_json(self.file_path)
        self.log.info("Finishing the twitter operator...")
