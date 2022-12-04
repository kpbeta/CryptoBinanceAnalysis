#!/bin/bash
echo "===================================================================" >> outlog.txt
date >> outlog.txt
echo "--------------------------------------------------------------------" >> outlog.txt
python3 /root/crypto/scrape.py >> outlog.txt

echo "--------------------------------------------------------------------" >> outlog.txt
echo "Completed at:" >> outlog.txt
date >> outlog.txt
echo "--------------------------------------------------------------------" >> outlog.txt
echo "" >> outlog.txt
echo "" >> outlog.txt
