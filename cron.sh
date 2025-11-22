#!/bin/bash

echo "*/10 * * * * cd /root/binance_airdrop && uv run python binance_airdrop.py >> /root/binance_airdrop/cron.log 2>&1" | crontab -