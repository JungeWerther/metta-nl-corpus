"""Utilities for versioning MeTTa space files."""

import hashlib
import subprocess
from pathlib import Path
from typing import NamedTuple

from structlog import getLogger

logger = getLogger(__name__)


class SpaceVersion(NamedTuple):
    """Version information for a MeTTa space file."""

    file_hash: str  # MD5 hash of the file content
    git_commit_hash: str | None  # Git commit hash if file is tracked


def compute_file_hash(file_path: Path) -> str:
    """
    Compute MD5 hash of a file.

    Args:
        file_path: Path to the file

    Returns:
        MD5 hash as hexadecimal string
    """
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_git_commit_hash(file_path: Path) -> str | None:
    """
    Get the git commit hash for the last commit that modified a file.

    Args:
        file_path: Path to the file

    Returns:
        Git commit hash or None if not in a git repo or file not tracked
    """
    try:
        result = subprocess.run(
            ["git", "log", "-n", "1", "--pretty=format:%H", "--", str(file_path)],
            cwd=file_path.parent,
            capture_output=True,
            text=True,
            timeout=5,
        )

        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()

        logger.warning(
            "Could not get git commit hash for file",
            file_path=str(file_path),
            returncode=result.returncode,
        )
        return None

    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        logger.warning(
            "Failed to get git commit hash",
            file_path=str(file_path),
            error=str(e),
        )
        return None


def get_space_version(space_file_path: Path) -> SpaceVersion:
    """
    Get version information for a MeTTa space file.

    Args:
        space_file_path: Path to the MeTTa space file

    Returns:
        SpaceVersion with file hash and git commit hash
    """
    if not space_file_path.exists():
        raise FileNotFoundError(f"Space file not found: {space_file_path}")

    file_hash = compute_file_hash(space_file_path)
    git_hash = get_git_commit_hash(space_file_path)

    logger.info(
        "Computed space version",
        file_path=str(space_file_path),
        file_hash=file_hash,
        git_hash=git_hash,
    )

    return SpaceVersion(file_hash=file_hash, git_commit_hash=git_hash)
