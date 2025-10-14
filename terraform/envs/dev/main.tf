terraform{
  required_version = ">=1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

module "jobs" {
  source = "../../modules/glue_jobs"
  for_each = var.script_s3_map      # job_name -> s3://...zip

  job_name = each.key
  role_arn = var.glue_role_arn
  script_s3_path = each.value
  glue_version = "4.0"
  worker_type = "G.1X"
  number_of_workers = 2
}
