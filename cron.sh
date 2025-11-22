#!/bin/bash

echo '*/10 * * * * cd /root/bianace_airdrop && /root/bianace_airdrop/venv/bin/python bianace_airdrop.py >> /root/bianace_airdrop/cron.log 2>&1' | crontab -