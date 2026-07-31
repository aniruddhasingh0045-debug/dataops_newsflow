#!/bin/bash
set -e

LOG=logs/etl.log

log() { echo "[$(date)] $1" | tee -a "$LOG"; }
trap 'log "ERROR at line $LINENO"' ERR

download_news() {
  log "Fetching news..."
  curl -sL "$1" -o data/raw/news_$(date +%Y%m%d).json
  log "Downloaded successfully"
}

validate_file() {
  local f="data/raw/news_$(date +%Y%m%d).json"
  [[ -s "$f" ]] || { log "File is empty!"; exit 1; }
  log "File is valid"
}

log "Starting ETL"
download_news "https://newsapi.org/v2/top-headlines?country=us&apiKey=$NEWS_API_KEY"
validate_file
log "Done"
