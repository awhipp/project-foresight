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


if __name__ == "__main__":
    # Get commit messages
    get_commit_messages = get_commit_messages()

    # Output new line separated commit messages
    print("\n".join(get_commit_messages))
