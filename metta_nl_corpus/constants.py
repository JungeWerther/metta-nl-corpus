from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
ANNOTATIONS_PATH = PROJECT_ROOT / "datasets" / "annotations.parquet"
VALIDATIONS_PATH = PROJECT_ROOT / "datasets" / "validations.parquet"
ANNOTATION_GUIDELINE_PATH = PROJECT_ROOT / "documentation" / "annotation_guideline.md"
