#!/usr/bin/env bash

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

# Setup logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Trap signals
cleanup() {
    log "Received signal, shutting down container..."
    exit 0
}

trap cleanup SIGTERM SIGINT

# Setup environment
setup_environment() {
    # Set Python path to app directory
    PYTHONPATH="${HOME}/app"
    export PYTHONPATH
    log "Environment setup completed"
}

# Container initialization
setup_container() {
    # Add any necessary container setup steps here
    # For example: checking required environment variables,
    # validating configs, setting up directories, etc.
    setup_environment
    
    # Return 0 for success, non-zero for failure
    return 0
}

# Main entrypoint
main() {
    log "Starting container"
    
    setup_container
    
    if ! wait_for_config; then
        log "unable to check for config file"
        exit 1
    fi

    if ! command -v slack-bot >/dev/null 2>&1; then
        log "slack-bot not found"
        exit 1
    fi

    if ! slack-bot; then
        log "Slack bot failed to start failed"
        exit 1
    fi

    # tail -f /dev/null
}

main "$@"