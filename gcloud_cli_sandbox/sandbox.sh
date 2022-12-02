# set defaulr account
gcloud config set core/account jdambrosio@qualesgroup.com
# list projects
gcloud projects list
# set current project
gcloud config set project linear-reporter-337312

# ERROR: (gcloud.iam.service-accounts.add-iam-policy-binding) INVALID_ARGUMENT: Role roles/bigquery.admin is not supported for this resource.
gcloud iam service-accounts add-iam-policy-binding \
    jj-poc-sfus@linear-reporter-337312.iam.gserviceaccount.com  \
    --member="user:jdambrosio@qualesgroup.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding linear-reporter-337312 \
    --member "serviceAccount:jj-poc-sfus@linear-reporter-337312.iam.gserviceaccount.com" \
    --role "roles/bigquery.admin"


ERROR: (gcloud.projects.add-iam-policy-binding) User [] does not have permission to access projects instance [:getIamPolicy] (or it may not exist): The caller does not have permission

