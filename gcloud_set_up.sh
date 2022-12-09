#!/bin/bash

account=analialduarte@gmail.com
project_id=stock-fundamental-us
service_account_name=terraform
service_account="${service_account_name}@${project_id}.iam.gserviceaccount.com"
service_account_key_path="$HOME/.google/credentials/google_credentials.json"
billing_account="0X0X0X-0X0X0X-0X0X0X"

# log in 
gcloud auth application-default login
# set default account
gcloud config set core/account "$account"
# list projects
gcloud projects list
# set current project
gcloud config set project "$project_id"

# create service account
gcloud iam service-accounts create "$service_account_name" \
    --description="IaC usage service account" \
    --display-name="${service_account_name} service account"

# set roles for the service account
gcloud projects add-iam-policy-binding "$project_id" \
    --member="serviceAccount:${service_account}" \
    --role="roles/bigquery.admin"

# set roles for the service account
gcloud projects add-iam-policy-binding "$project_id" \
    --member="serviceAccount:${service_account}" \
    --role="roles/storage.admin"

# set roles for the service account
gcloud projects add-iam-policy-binding "$project_id" \
    --member="serviceAccount:${service_account}" \
    --role="roles/storage.objectAdmin"

# download service account key 
gcloud iam service-accounts keys create "$service_account_key_path" --iam-account=$service_account

# list billing accounts
gcloud beta billing accounts list

# link billing account to project
gcloud beta billing accounts projects link "$project_id" --billing-account="$billing_account"
