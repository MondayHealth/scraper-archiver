# scraper-archiver
write scrape info to a SQLite3 DB and upload that db to s3

1) `source venv/bin/activate`
2) `pip install -r requirements.txt`
3) `python main.py`
4) `./upload.sh`

You may then choose whether or not to delete `./archive` because it may be quite large (1-10gb). Running `main.py` again will overwrite existing files made that day.
