terraform{
  required_version = ">=1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version ="~>5.0"}
  }
}

module "jobs" {
source = "terraform/modules/glue_jobs"
for each = var.script_s3_map      # job_name -> script zip

job_name = each.key
role_arn = var.glue_role_arn
script_s3 = each.value
glue_version = "4.0"
worker_type = "G.1X"
number_of_workers = 2

}
