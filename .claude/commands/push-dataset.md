Push the MeTTa-NL-Corpus annotations dataset to HuggingFace Hub.

Run: `uv run python main.py push-dataset --repo-id JungeWerther/metta-nl-corpus`

This exports annotations from the SQLite store and uploads them as a parquet file.

If the user specifies a different repo or wants validations too, adjust flags accordingly:
- `--table validations` to push validations
- `--private` to make the repo private
- `--repo-id <id>` to push to a different repo
