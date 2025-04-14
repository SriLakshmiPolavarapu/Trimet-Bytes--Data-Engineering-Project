#!/bin/bash

# Schedule VM to start/stop automatically for DataEng S25 Project
# Project: dataengineeringproject-456307
# VM: instance-20250409-070924

# Create instance schedule
gcloud compute resource-policies create instance-schedule daily-vm-schedule \
  --description="Daily VM start/stop for DataEng project" \
  --vm-start-schedule="0 1 * * *" \
  --vm-stop-schedule="30 2 * * *" \
  --timezone="America/Los_Angeles" \
  --region="us-west1"

# Attach schedule to VM
gcloud compute instances add-resource-policy instance-20250409-070924 \
  --policy-name=daily-vm-schedule \
  --zone="us-west1-a"
