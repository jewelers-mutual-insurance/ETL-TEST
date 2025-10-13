variable "glue_role_arn" { type = string }
variable "script_s3_map" { type = map(string) } # job_name -> s3://...zip
