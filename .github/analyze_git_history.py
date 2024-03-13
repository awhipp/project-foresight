"""Python script to analyze git history and determine version bump."""

import subprocess


def get_commit_messages():
    """Get commit messages using git log."""
    # Get commit messages using git log
    commit_log = (
        subprocess.check_output(["git", "log", "--pretty=%s"]).decode().split("\n")
    )
    # Filter out empty lines
    commit_messages = [msg.strip() for msg in commit_log if msg.strip()]

    # Only get the commit messages between the last two merge requests
    merge_request_count = 0
    valid_commit_messages = []
    for _, message in enumerate(commit_messages):
        if merge_request_count == 2:
            break
        if "Merge pull request" in message:
            merge_request_count += 1
        else:
            valid_commit_messages.append(message)

    return valid_commit_messages


def determine_version_bump(valid_messages):
    """Determine version bump based on commit messages."""
    version_bump = None

    # Check commit messages for specific keywords to determine version bump
    for message in valid_messages:
        if message.startswith("fix") and version_bump in [None, "patch"]:
            version_bump = "patch"
        elif message.startswith("feat") and version_bump in [None, "patch", "minor"]:
            version_bump = "minor"
        elif message.startswith("BREAKING CHANGE") and version_bump in [
            None,
            "patch",
            "minor",
            "major",
        ]:
            version_bump = "major"
        # Add more conditions based on your project's conventions

    # Default to 'patch' if no specific keyword found
    return version_bump


if __name__ == "__main__":
    # Determine version bump
    version_bump = determine_version_bump(get_commit_messages())

    # Output version bump
    print(version_bump)
