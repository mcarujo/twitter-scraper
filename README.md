# Twitter Scraper

```python
twitter_operator = TwitterOperator(
    task_id="twitter_extract_bbb22",
    conn_id="twitter_default",
    query="BBB22", ## Search/Query
    file_path="outputs/twitter_bbb22_2022_01_21.csv",
    start_time="2022-01-21T00:00:00.00Z", # Starts when
    end_time="2022-01-21T23:59:59.00Z", # Ends when
)
```
